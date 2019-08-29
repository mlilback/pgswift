//
//  Connection.swift
//  pgswift
//
//  Created by Mark Lilback on 8/26/19.
//

import Foundation
import CLibpq

public final class Connection {
	
	public typealias PGConnection = OpaquePointer
	
	/// the info this connection was created with
	let connectInfo: ConnectInfo
	/// the low-level PGConnection object from libpq
	public private(set) var pgConnection: PGConnection?
	
	/// designatedc initializer.
	///
	/// - Parameter connectInfo: the arguments that will be used to open a connection
	public init(connectInfo: ConnectInfo) {
		self.connectInfo = connectInfo
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

	/// opens the connection to the database server
	public func open() throws {
		guard pgConnection == nil else { throw PostgreSQLStatusErrors.alreadyOpen }
		connectInfo.withParamterCStrings { (keywords, values) -> () in
			pgConnection = PQconnectdbParams(keywords, values, 0)
		}
		precondition(pgConnection != nil, "PQconnect returned nil, which means failed to alloc memory which should be  impossible")
	}
	
	/// closes the connection to the database server
	public func close() {
		guard pgConnection != nil  else { return }
		PQfinish(pgConnection)
		pgConnection = nil
	}
	
	// MARK: status
	
	public var lastErrorMessage: String {
		guard let err = PQerrorMessage(pgConnection) else { return "" }
		return String(cString: err)
	}
	
	/// true if the connection is currently open
	public var isConnected: Bool {
		return pgConnection != nil && PQstatus(pgConnection!) == CONNECTION_OK
	}
	
	/// Throws an error if the connection is nil or not connected
	func validateConnection() throws {
		guard pgConnection != nil else { throw PostgreSQLError(code: .connectionDoesNotExist, connection: self) }
		guard isConnected else { throw PostgreSQLError(code: .connectionFailure, connection: self) }
	}

	/// Returns the version of the server. Only works if a connection is open.
	///
	/// - Returns: the server version string
	public func serverVersion() throws -> String {
		guard isConnected else { throw PostgreSQLError(code: .connectionDoesNotExist, connection: self) }
		return String(utf8String: PQparameterStatus(pgConnection, "server_version")) ?? "unknown"
	}
	
	// MARK: sql execution
	
	public func execute(query: String) throws -> PGResult {
		guard isConnected else { throw PostgreSQLError(code: .connectionDoesNotExist , connection: self) }
		let rawResult: OpaquePointer? = PQexec(pgConnection, query)
		guard let result = rawResult else { throw PostgreSQLStatusErrors.badResponse }
		return PGResult(result: result, connection: self)
	}
	
	// MARK: Notify/Listen

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
		let sock = PQsocket(self.pgConnection)
		guard sock >= 0 else {
			throw PostgreSQLError(code: .ioError, reason: "failed to get socket for connection")
		}
		let src = DispatchSource.makeReadSource(fileDescriptor: sock, queue: queue)
		src.setEventHandler { [weak self] in
			guard let strongSelf = self else { return }
			guard strongSelf.pgConnection != nil else {
				callback(nil, PostgreSQLError(code: .connectionDoesNotExist, reason: "connection does not exist"))
				return
			}
			PQconsumeInput(strongSelf.pgConnection)
			while let pgNotify = PQnotifies(strongSelf.pgConnection) {
				let notification = PGNotification(pgNotify: pgNotify.pointee)
				callback(notification, nil)
				PQfreemem(pgNotify)
			}
		}
//		try self.execute("LISTEN \(channel)")
		return src
	}
}
