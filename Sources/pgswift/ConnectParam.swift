//
//  ConnectParams.swift
//  pgswift
//
//  Created by Mark Lilback on 8/25/19.
//

import Foundation

/// A single parameter to open a connection to a postgresql server
public struct ConnectParam {
	/// supported names for connection parameters. Defined at [PostgreSQL docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).
	public enum Name: String, CaseIterable {
		/// host running the database server
		case host
		/// port on host to connect to. Defaults to 5432. Passed as String becuase that's what the native PostgreSQL client uses
		case port
		/// the name of the database to use
		case dbname
		/// name to login with
		case user
		/// password to login with
		case password
		/// how long (in seconds) to wait before giving up on opening a connection
		case connectTimeout = "connect_timeout"
		/// psuedo command line parameters
		case options
		/// the name of the application connecting. Shown in admin functions.
		case appName = "application_name"
		/// which SSLMode to use
		case sslMode = "sslmode"
	}

	/// possible modes for sslMode parameter. Defined at [PostgreSQL docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).
	public enum SSLMode: String, CaseIterable {
		/// onlyy try a non-SSL connection
		case disable
		/// first try a non-SSL connection, if that fails, try a SSL connection
		case allow
		/// first try an SSL connection, if that fails, try a non-SSQL connection
		case prefer
		/// only try an SSL connection. If a root CA file is present, verify the certificate in the same way as if verify-ca was specified. This is the default value
		case require
		/// only try an SSL connection, and verify that the server certificate is issued by a trusted certificate authority (CA)
		case verifyCA = "verify-ca"
		/// only try an SSL connection, verify that the server certificate is issued by a trusted CA and that the requested server host name matches that in the certificate
		case verifyFull = "verify-full"
	}
	
	/// Name of the parameter
	public let name: Name
	/// Value of the parameter
	public let value:  String
	
	/// creates a ConnectionParam with a key and value
	///
	/// - Parameters:
	///   - name: the name of the parameter
	///   - value: the value of the parameter
	public init(_ name: Name, _ value: String) {
		self.name = name
		self.value = value
	}	

	/// initializer for a sslMode parameter
	///
	/// - Parameters:
	///   - sslMode: The SSL mode to use
	public init(sslMode: SSLMode) {
		self.name = .sslMode
		self.value = sslMode.rawValue
	}
}

/// Encapsulates a set of connection parameters
public struct ConnectInfo {
	/// the parameters included in this structure
	public private(set) var params: [ConnectParam]
	
	/// initalizes with an array of parameters
	///
	/// - Parameter parameters: array of parameters
	public init(parameters: [ConnectParam]) {
		self.params = parameters
	}
	
	/// initialize with basic parameters
	///
	/// - Parameters:
	///   - host: the hostname to connect to
	///   - port: the port to connect on as a string. Defaults to "5432"
	///   - user: the user to connect as
	///   - password: the password for user. Defaults to an empty string
	///   - dbname: the name of the database to oonnect to
	///   - sslMode: the SSL mode to use. Defaults to .prefer
	public init(host: String = "localhost", port: String = "5432",
		user: String, password: String = "", dbname: String, sslMode:
		ConnectParam.SSLMode = .prefer)
	{
		self.init(parameters: [
			ConnectParam(.host, host),
			ConnectParam(.port, port),
			ConnectParam(.user, user),
			ConnectParam(.password, password),
			ConnectParam(.dbname, dbname),
			ConnectParam(.sslMode, sslMode.rawValue)
		])
	}
	
	///Turns params into cstrings with a final null string at the end.  Based on [swift sources](https://github.com/apple/swift/stdlib/private/SwiftPrivate/SwiftPrivate.swift)
	func withParamterCStrings(_ body: ([UnsafePointer<CChar>?], [UnsafePointer<CChar>?]) -> Void)
	{
		// bytes necessary for each array, including null terminator for each string
		let namesCounts = Array(params.map { $0.name.rawValue.utf8.count + 1 } )
		let valuesCounts = Array(params.map { $0.value.utf8.count + 1 } )
		let namesOffsets = [0] + scan(namesCounts, 0, +)
		let valuesOffsets = [0] + scan(valuesCounts, 0, +)
		let namesBufferSize = namesOffsets.last!
		let valuesBufferSize = valuesOffsets.last!
		
		var namesBuffer: [UInt8] = []
		namesBuffer.reserveCapacity(namesBufferSize)
		var valuesBuffer: [UInt8] = []
		valuesBuffer.reserveCapacity(valuesBufferSize)
		for param in params {
			namesBuffer.append(contentsOf: param.name.rawValue.utf8)
			namesBuffer.append(0)
			valuesBuffer.append(contentsOf: param.value.utf8)
			valuesBuffer.append(0)
		}

		return namesBuffer.withUnsafeMutableBufferPointer { ( namesBuffer:  inout UnsafeMutableBufferPointer<UInt8>) -> () in
			return valuesBuffer.withUnsafeMutableBufferPointer { (valuesBuffer: inout UnsafeMutableBufferPointer<UInt8>) -> () in
				let namesPtr = UnsafeMutableRawPointer(namesBuffer.baseAddress!).bindMemory(to: CChar.self, capacity: namesBuffer.count)
				let valuesPtr = UnsafeMutableRawPointer(valuesBuffer.baseAddress!).bindMemory(to: CChar.self, capacity: valuesBuffer.count)
				var namesCStrings: [UnsafePointer<CChar>?] = namesOffsets.map { UnsafePointer(namesPtr + $0) }
				namesCStrings[namesCStrings.count - 1] = nil
				var valuesCStrings: [UnsafePointer<CChar>?] = valuesOffsets.map { UnsafePointer(valuesPtr + $0) }
				valuesCStrings[valuesCStrings.count - 1] = nil
				body(namesCStrings, valuesCStrings)
			}
		}
	}
	
	/// Compute the prefix sum of `seq`. From [swift sources](https://github.com/apple/swift/stdlib/private/SwiftPrivate/SwiftPrivate.swift)
	func scan<
		S : Sequence, U
		>(_ seq: S, _ initial: U, _ combine: (U, S.Iterator.Element) -> U) -> [U] {
		var result: [U] = []
		result.reserveCapacity(seq.underestimatedCount)
		var runningResult = initial
		for element in seq {
			runningResult = combine(runningResult, element)
			result.append(runningResult)
		}
		return result
	}
}
