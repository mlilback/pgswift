//
//  Results.swift
//  pgswift
//
//  Created by Mark Lilback on 8/28/19.
//

import Foundation
import CLibpq

public class PGResult {
	// MARK: - properties
	
	private let result: OpaquePointer
	weak var connection: Connection?
	public let status: Status
	private let dateFormatter: ISO8601DateFormatter
	private let timeFormatter: ISO8601DateFormatter
	private let timestampFormatter: DateFormatter
	
	/// true if the result was .commandkOk, .tuplesOk, or .signleTupe
	public var wasSuccessful: Bool {
		return status == .commandOk || status == .tuplesOk || status == .singleTuple
	}

	public var statusMessage: String { return String(cString: PQresStatus(status.pgStatus)) }
	public var errorMessage: String { return String(cString: PQresultErrorMessage(result)) }
	public var rowCount: Int { return Int(PQntuples(result)) }
	public var columnCount: Int { return Int(PQnfields(result)) }
	public var returnedData: Bool { return status == .tuplesOk || status == .singleTuple }
	public var rowsAffected: String { return String(utf8String: PQcmdTuples(result)) ?? "" }
	/// if a single row was inserted, the Oid of that row. Returns -1 of there is no value
	public var insertedOid: Int {
		let val = PQoidValue(result)
		if val == InvalidOid { return -1 }
		return Int(val)
	}
	
	/// names of columns indexed by column number
	public let columnNames: [String]
	/// the type of the column
	public let columnTypes: [PGType]
	
	/// true if raw data is a string, false for binary
	let columnFormats: [ColumnFormat]
	
	// MARK: - init/deinit
	
	init(result: OpaquePointer, connection: Connection) {
		self.result = result
		self.connection = connection
		self.status = Status(result)
		let colCount = Int(PQnfields(result))
		columnNames = (0..<colCount).map { return String(utf8String: PQfname(result, Int32($0))) ?? "" }
		columnFormats = (0..<colCount).map { return PQfformat(result, Int32($0))  == 0 ? .string : .binary }
		columnTypes = (0..<colCount).map { return PGType(rawValue: PQftype(result, Int32($0))) ?? .unsupported }
		dateFormatter = ISO8601DateFormatter()
		dateFormatter.formatOptions = [.withFullDate, .withDashSeparatorInDate]
		timeFormatter = ISO8601DateFormatter()
		timeFormatter.formatOptions = [.withTime, ]
		timestampFormatter = DateFormatter()
		timestampFormatter.dateFormat = " yyyy-MM-dd H:m:s.SSSSSSxx"
		timestampFormatter.locale = Locale(identifier: "en_US_POSIX")
		timestampFormatter.timeZone = TimeZone(secondsFromGMT: 0)
//		timestampFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
	}
	
	deinit {
		PQclear(result)
	}

	// MARK: - get values from a row
	
	/// returns the value as the actual native type (String, Int, Bool, etc.)
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value
	/// - Throws: if any parameter is invalid, if the column's NativeType doesn't match T
	public func getValue<T>(row: Int, column: Int) throws -> T? {
		guard PQgetisnull(result, Int32(row), Int32(column)) == 0 else { return nil }
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		guard row < rowCount else { throw PostgreSQLStatusErrors.invalidRowNumber }
		guard columnTypes[column].nativeType.metaType() == T.self
			else { throw PostgreSQLStatusErrors.invalidType }

		switch columnTypes[column].nativeType {
		case .bool:
			return try getBoolValue(row: row, column: column) as? T
		case .int:
			return try getIntValue(row: row, column: column) as? T
		case .float:
			return try getFloatValue(row: row, column: column) as? T
		case .double:
			return try getDoubleValue(row: row, column: column) as? T
		case .string:
			return try getStringValue(row: row, column: column) as? T
		case .date:
			return try getDateValue(row: row, column: column) as? T
		case .data:
			return try getDataValue(row: row, column: column) as? T
		}
	}
	
	/// finds the index of the first column matching columnName  then calls getValue(from:column:) with it
	///
	/// - Parameters:
	///   - row: row number
	///   - columnName: column name
	/// - Returns: the value
	/// - Throws: if any parameter is invalid, if there is no column with the specified name,
	///           or if the column's NativeType doesn't match T
	public func getValue<T>(row: Int, columnName: String) throws -> T? {
		let possibleColNum = columnNames.firstIndex { $0.caseInsensitiveCompare(columnName) == .orderedSame }
		guard let colNum = possibleColNum else { throw PostgreSQLStatusErrors.invalidColumnName }
		return try getValue(row: row, column: colNum)
	}
	
	/// Gets the specified value as a data object.
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a string, or nil if NULL
	/// - Throws: if an invalid column number
	public func getDataValue(row: Int, column: Int) throws -> Data? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }

		let size = Int(PQgetlength(result, Int32(row), Int32(column)))
		guard let value = try setupValue(row: row, column: column) else { return nil }

		if columnFormats[column] == .string {
			var length: Int = 0
			return try value.withMemoryRebound(to: CChar.self, capacity: size) { ptr in
				guard let ptr = PQunescapeBytea(String(utf8String: ptr), &length)
					else {throw PostgreSQLError(code: .outOfMemory, errorMessage: "failed to unescape binary value") }
				let raw = UnsafeRawPointer(ptr)
				return Data(bytes: raw, count: size)
			}
		} else {
			let rawValue = UnsafeRawPointer(value)
			return Data(bytes: rawValue, count: size)
		}
	}
	
	/// Gets the specified value as a string. This works for more types than nativeType.string
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a string, or nil if NULL
	/// - Throws: if value not easily convertible to a string, or if an invalid column number
	public func getStringValue(row: Int, column: Int) throws -> String? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		// binary and string format are the same
		// guard PQfformat(result, Int32(column)) == 0 else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		let size = Int(PQgetlength(result, Int32(row), Int32(column)))
		return rawValue.withMemoryRebound(to: CChar.self, capacity: size) { ptr in
			return String(validatingUTF8: ptr)
		}
	}
	
	/// Gets the specified value as a bool if nativeType is .bool
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a Bool, or nil if NULL
	/// - Throws: if value not a bool, or if an invalid column number
	public func getBoolValue(row: Int, column: Int) throws -> Bool? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		guard columnTypes[column].nativeType == .bool else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		if columnFormats[column] == .string {
			return rawValue.pointee == "t".utf8CString[0]
		}
		return rawValue.withMemoryRebound(to: Bool.self, capacity: 1) { $0.pointee }
	}
	
	/// Gets the specified value as a date if columnType.nativeType == .date
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a date, or nil if NULL
	/// - Throws: if native format is not a date, or if an invalid column number
	public func getDateValue(row: Int, column: Int) throws -> Date? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		guard columnTypes[column].nativeType == .date else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		if columnFormats[column] == .string {
			guard let dateStr = try getStringValue(row: row, column: column) else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
			switch columnTypes[column] {
				case .date: // set dates to noon so timezone won't revert to previous day
					return dateFormatter.date(from: dateStr)?.addingTimeInterval(12.0 * 60.0 * 60.0)
				case .time:
					fallthrough
				case .timetz:
					return timeFormatter.date(from: dateStr)
				case .timestamp:
					fallthrough
				case .timestamptz:
					return timestampFormatter.date(from: dateStr)
				default:
					throw PostgreSQLStatusErrors.unsupportedDataFormat
			}
		}
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		if columnTypes[column] == .date {
			let days = Int32(bigEndian: rawValue.withMemoryRebound(to: Int32.self, capacity: 1) { $0.pointee })
			let timeInterval = TimeInterval(days * BinaryUtilities.DateTime.secondsInDay)
			return Date(timeInterval: timeInterval, since: BinaryUtilities.DateTime.referenceDate)
		}
		let microseconds = Int64(bigEndian: rawValue.withMemoryRebound(to: Int64.self, capacity: 1) { (ptr) in
			return ptr.pointee
 		})
		let interval = TimeInterval(microseconds) / 1_000_000
		return Date(timeInterval: interval, since: BinaryUtilities.DateTime.referenceDate)
	}
	
	/// Gets the specified value as an integer if columnType.nativeType == .int
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as an integer, or nil if NULL
	/// - Throws: if native format is not an integer, or if an invalid column number
	public func getIntValue(row: Int, column: Int) throws -> Int? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		if columnFormats[column] == .string {
			guard let val = try? getStringValue(row: row, column: column), val.count > 0 else { return nil }
			return Int(val)
		}
		guard columnTypes[column].nativeType == .int else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		switch columnTypes[column] {
		case .int2:
			 return Int(Int16(bigEndian: rawValue.withMemoryRebound(to: Int16.self, capacity: 1) { $0.pointee }))
		case .int4:
			return Int(Int32(bigEndian: rawValue.withMemoryRebound(to: Int32.self, capacity: 1) { $0.pointee }))
		case .int8:
			return Int(bigEndian: rawValue.withMemoryRebound(to: Int.self, capacity: 1) { $0.pointee })
		default:
			throw PostgreSQLStatusErrors.unsupportedDataFormat
		}
	}
		
	/// Gets the specified value as a float if columnType.nativeType == .float
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a float, or nil if NULL
	/// - Throws: if native format is not float, or if an invalid column number
	public func getFloatValue(row: Int, column: Int) throws -> Float? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		if columnFormats[column] == .string {
			guard let val = try? getStringValue(row: row, column: column), val.count > 0 else { return nil }
			return Float(val)
		}
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		let uintValue = rawValue.withMemoryRebound(to: UInt32.self, capacity: 1) { ptr in
			return ptr.pointee
		}
		return Float(bitPattern: UInt32(bigEndian: uintValue))
	}
	
	/// Gets the specified value as a double if columnType.nativeType == .double or .float
	///
	/// - Parameters:
	///   - row: row number
	///   - column: column number
	/// - Returns: the value as a double, or nil if NULL
	/// - Throws: if native format is not an double or float, or if an invalid column number
	public func getDoubleValue(row: Int, column: Int) throws -> Double? {
		guard column < columnCount else { throw PostgreSQLStatusErrors.invalidColumnNumber }
		if columnFormats[column] == .string {
			guard let val = try? getStringValue(row: row, column: column), val.count > 0 else { return nil }
			return Double(val)
		}
		guard let rawValue = try setupValue(row: row, column: column) else { return nil }
		let uintValue = rawValue.withMemoryRebound(to: UInt64.self, capacity: 1) { ptr in
			return ptr.pointee
		}
		return Double(bitPattern: UInt64(bigEndian: uintValue))
	}
	
	// MARK: - private methods
	
	@inline(__always)
	private func setupValue(row: Int, column: Int) throws -> UnsafePointer<UInt8>? {
		guard column < columnNames.count else { throw PostgreSQLError(code: .numericValueOutOfRange, connection: connection!) }
		let colNum = Int32(column)
		let rowNum = Int32(row)
		guard PQgetisnull(result, rowNum, colNum) == 0 else { return nil }
		return getRawValue(row: rowNum, col: colNum)
	}
	
	/// gets the value at the col and row as an pointer convertible to a string/data
	/// row/col must have been validated
	private func getRawValue(row: Int32, col: Int32) -> UnsafePointer<UInt8>? {
		let rawPtr =  UnsafeRawPointer(PQgetvalue(result, row, col)!)
		return rawPtr.assumingMemoryBound(to: UInt8.self)

	}
	
	// MARK: - enums
	
	/// possible wire formats for data in a column
	enum ColumnFormat: Int {
		case string
		case binary
	}
	
	/// the possible status returned from the server
	public enum Status {
		case commandOk
		case tuplesOk
		case copyOut
		case copyIn
		case copyBoth
		case badResponse
		case nonFatalError
		case fatalError
		case emptyQuery
		case singleTuple
		
		/// Creates a status enum value
		///
		/// - Parameter pointer: the PQresult
		init(_ pointer: OpaquePointer?) {
			guard let pointer = pointer else {
				self = .fatalError
				return
			}
			
			switch PQresultStatus(pointer) {
			case PGRES_COMMAND_OK:
				self = .commandOk
			case PGRES_TUPLES_OK:
				self = .tuplesOk
			case PGRES_COPY_OUT:
				self = .copyOut
			case PGRES_COPY_IN:
				self = .copyIn
			case PGRES_COPY_BOTH:
				self = .copyBoth
			case PGRES_BAD_RESPONSE:
				self = .badResponse
			case PGRES_NONFATAL_ERROR:
				self = .nonFatalError
			case PGRES_FATAL_ERROR:
				self = .fatalError
			case PGRES_EMPTY_QUERY:
				self = .emptyQuery
			case PGRES_SINGLE_TUPLE:
				self = .singleTuple
			default:
				self = .fatalError
			}
		}
		
		/// the matching PostgreSQL status
		public var pgStatus :  ExecStatusType {
			switch self {
			case .commandOk: return PGRES_COMMAND_OK
			case .tuplesOk: return PGRES_TUPLES_OK
			case .copyOut: return PGRES_COPY_OUT
			case .copyIn: return PGRES_COPY_IN
			case .copyBoth: return PGRES_COPY_BOTH
			case .badResponse: return PGRES_BAD_RESPONSE
			case .nonFatalError: return PGRES_NONFATAL_ERROR
			case .fatalError: return PGRES_FATAL_ERROR
			case .emptyQuery: return PGRES_EMPTY_QUERY
			case .singleTuple: return PGRES_SINGLE_TUPLE
			}
		}
	}
}
