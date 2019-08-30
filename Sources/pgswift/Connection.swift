//
//  Connection.swift
//  pgswift
//
//  Created by Mark Lilback on 8/26/19.
//

import Foundation
import CLibpq

public final class Connection {
	// MARK: - properties
	public typealias PGConnection = OpaquePointer?
	
	/// if dates are stored as integer. If false, stored as float.
	private(set) var hasIntegerDatetimes: Bool = false
	/// the info this connection was created with
	let connectInfo: ConnectInfo
	/// the low-level PGConnection object from libpq
	private var pgConnection: PGConnection?
	
	private static var nextQueueNumber = 1
	private let conQueue: DispatchQueue
	
	// MARK: - init/deinit
	
	/// designated initializer.
	///
	/// - Parameter connectInfo: the arguments that will be used to open a connection
	public init(connectInfo: ConnectInfo) {
		self.connectInfo = connectInfo
		self.conQueue = DispatchQueue(label: "connection queue \(Connection.nextQueueNumber)")
		Connection.nextQueueNumber += 1
	}
	
	/// Initializes with a fixed set of connection parameters
	///
	/// - Parameters:
	///   - host: the host to connect to. Defaults to "localhost"
	///   - port: The port to connect to, as a string. Defaults to "5432"
	///   - user: the user to connect as
	///   - password: the password for user. Defauls to an empty string
	///   - dbname: the name of the ddatabase to connect to
	///   - sslMode: the SSL Mode to use. Defaults to .prefer
	public convenience init(host: String = "localhost", port: String = "5432",
				user: String, password: String = "", dbname: String, sslMode:
		ConnectParam.SSLMode = .prefer)
	{
		let params = [
			ConnectParam(.host, host),
			ConnectParam(.port, port),
			ConnectParam(.user, user),
			ConnectParam(.password, password),
			ConnectParam(.dbname, dbname),
			ConnectParam(.sslMode, sslMode.rawValue)
		]
		self.init(connectInfo: ConnectInfo(parameters: params))
	}
	
	deinit {
		close()
	}
	
	// MARK: - open/close
	
	/// opens the connection to the database server
	public func open() throws {
		try conQueue.sync {
			guard pgConnection == nil else { throw PostgreSQLStatusErrors.alreadyOpen }
			connectInfo.withParamterCStrings { (keywords, values) -> () in
				pgConnection = PQconnectdbParams(keywords, values, 0)
			}
			precondition(pgConnection != nil, "PQconnect returned nil, which means failed to alloc memory which should be  impossible")
			hasIntegerDatetimes = getBooleanParameter(key: "integer_datetimes", defaultValue: true)
		}
	}
	
	/// closes the connection to the database server
	public func close() {
		conQueue.sync {
			guard let pgcon = pgConnection  else { return }
			PQfinish(pgcon)
			pgConnection = nil
		}
	}
	
	// MARK: - status
	
	public var lastErrorMessage: String {
		return conQueue.sync {
			guard let pgcon = pgConnection, let err = PQerrorMessage(pgcon) else { return "" }
			return String(validatingUTF8: err) ?? ""
		}
	}
	
	/// true if the connection is currently open
	public var isConnected: Bool {
		return conQueue.sync {
			return pgConnection != nil && PQstatus(pgConnection!) == CONNECTION_OK
		}
	}
	
	/// for internal use from inside the conQueue.
	private var isConnectedRaw: Bool {
		return pgConnection != nil && PQstatus(pgConnection!) == CONNECTION_OK
	}
	
	/// Throws an error if the connection is nil or not connected
	func validateConnection() throws {
		try conQueue.sync {
			guard pgConnection != nil else { throw PostgreSQLError(code: .connectionDoesNotExist, connection: self) }
			guard isConnectedRaw else
				{ throw PostgreSQLError(code: .connectionFailure, connection: self) }
		}
	}

	/// Returns the version of the server. Only works if a connection is open.
	///
	/// - Returns: the server version string
	public func serverVersion() throws -> String {
		guard let sver: String =  try getSingleRowValue(query: "show server_version")
			else { fatalError("failed to get server version") }
		return sver
	}
	
	// MARK: - convience methods
	
	/// Allows use of this connection's PGConnection synchronously
	///
	/// - Parameter body: closure called with the PG connection
	public func withPGConnection(body: (PGConnection) -> Void) {
		guard isConnected, let pgcon = pgConnection else
		{ precondition(isConnected, "database not connected"); return }
		conQueue.sync {
			body(pgcon)
		}
	}
	
	// MARK: - private helper methods
	
	/// get a boolean parameter setting
	private func getBooleanParameter(key: String, defaultValue: Bool = false) -> Bool {
		guard isConnected, let pgcon = pgConnection
			else { return false }

		guard let value = PQparameterStatus(pgcon, key) else { return defaultValue }
		return String(cString: value) == "on"
	}
	
	// MARK: - sql execution
	
	/// execute query without locking (to be called when already locked)
	@discardableResult
	private func executeRaw(query: String) throws -> PGResult {
		guard isConnectedRaw, let pgcon = pgConnection else
			{ throw PostgreSQLError(code: .connectionDoesNotExist , connection: self) }
		let rawResult: OpaquePointer? = PQexec(pgcon, query)
		guard let result = rawResult else { throw PostgreSQLStatusErrors.badResponse }
		return PGResult(result: result, connection: self)
	}

	/// Executes the query and returns the results
	///
	/// - Parameter query: query to perform
	/// - Returns: the results of that query
	/// - Throws: if connection isn't open, or don't get a valid response
	@discardableResult
	public func execute(query: String) throws -> PGResult {
		return try conQueue.sync {
			guard isConnectedRaw, let pgcon = pgConnection else {
				throw PostgreSQLError(code: .connectionDoesNotExist , errorMessage: "no connnection")
			}
			let rawResult: OpaquePointer? = PQexec(pgcon, query)
			guard let result = rawResult else { throw PostgreSQLStatusErrors.badResponse }
			return PGResult(result: result, connection: self)
		}
	}

	@discardableResult
	/// Execute the query and returns the results. Internally transfers data in binary format
	///
	/// - Parameter query: query to perform
	/// - Returns: the results of that query
	/// - Throws: if connection isn't open, or don't get a valid response
	public func executeBinary(query: String) throws -> PGResult {
		return try conQueue.sync {
			guard isConnectedRaw, let pgcon = pgConnection else {
				throw PostgreSQLError(code: .connectionDoesNotExist , errorMessage: "no connnection")
			}
			// no parameters, want binary back
			let rawResult: OpaquePointer? =  PQexecParams(pgcon, query, 0, nil, nil, nil, nil, 1)
			guard let result = rawResult else { throw PostgreSQLStatusErrors.badResponse }
			return PGResult(result: result, connection: self)
		}
	}

	/// Returns the value of row 0, column 0 of a query that returns 1 row and 1 column
	///
	/// - Parameters:
	///   - query: a query that should return 1 row with 1 column
	/// - Returns: the value
	/// - Throws: if the data types don't match, or an error executing query
	public func getSingleRowValue<T>(query: String) throws -> T? {
		guard query.count > 0 else { throw PostgreSQLStatusErrors.emptyQuery }
		return try conQueue.sync {
			let result = try executeRaw(query: query)
			guard result.columnCount == 1, result.rowCount == 1 else { throw PostgreSQLStatusErrors.invalidQuery }
			let colType = result.columnTypes[0]
			guard colType != .unsupported, colType.nativeType.matches(T.self)
				else { throw PostgreSQLStatusErrors.invalidType }
			let val: T? = try result.getValue(row: 0, column: 0)
			return val
		}
	}

	// MARK: - Notify/Listen

	/// Creates a dispatch read source for this connection that will call `callback` on `queue` when a notification is received
	///
	/// - Parameter channel: the channel to register for
	/// - Parameter queue: the queue to create the DispatchSource on
	/// - Parameter callback: the callback
	/// - Parameter notification: The notification received from the database
	/// - Parameter error: Any error while reading the notification. If not nil, the source will have been canceled
	/// - Returns: the dispatch socket to activate
	/// - Throws: if fails to get the socket for the connection
	public func listen(toChannel channel: String, queue: DispatchQueue, callback: @escaping (_ notification: PGNotification?, _ error: Error?) -> Void) throws -> DispatchSourceRead {
		return try conQueue.sync { () -> DispatchSourceRead in
			guard let pgcon = self.pgConnection else
				{ throw PostgreSQLError(code: .connectionDoesNotExist, errorMessage: "connection does not exist") }
			let sock = PQsocket(pgcon)
			guard sock >= 0 else {
				throw PostgreSQLError(code: .ioError, reason: "failed to get socket for connection")
			}
			let src = DispatchSource.makeReadSource(fileDescriptor: sock, queue: queue)
			src.setEventHandler { [weak self] in
				guard let strongSelf = self, strongSelf.isConnected else {
					callback(nil, PostgreSQLError(code: .connectionDoesNotExist, reason: "connection does not exist"))
					return
				}
				strongSelf.withPGConnection { con in
					PQconsumeInput(con)
					while let pgNotify = PQnotifies(con) {
						let notification = PGNotification(pgNotify: pgNotify.pointee)
						callback(notification, nil)
						PQfreemem(pgNotify)
					}
				}
			}
			try executeRaw(query: "LISTEN \(channel)")
			return src
		}
	}
}
