//
//  QueryParameter.swift
//  pgswift
//
//  Created by Mark Lilback on 8/31/19.
//

import Foundation

@available(OSX 10.13, *)
public final class QueryParameter {
	/// the type this parameter was requested to be
	public let valueType: PGType
	var columnFormat: PGResult.ColumnFormat {
		let stringTypes:  [PGType] = [.date, .timetz, .time, .timestamptz, .timestamp]
		if stringTypes.contains(valueType) { return .string }
		return .binary
	}

	private let bytes: UnsafePointer<Int8>
	public let valueCount: Int
	
	/// The caller owns this value and is responsible for calling .deallocate() on it
	public var valueBytes: UnsafePointer<Int8> {
		let mptr = UnsafeMutablePointer<Int8>.allocate(capacity: valueCount)
		mptr.initialize(from: bytes, count: valueCount)
		return UnsafePointer<Int8>(mptr)
	}
	
	/// Create a QueryParameter binding
	///
	/// - Parameters:
	///   - valueType: the SQL data type
	///   - value: the value to bind. Must be the matching NativeType
	///   - connection: connection this parameter belongs to
	/// - Throws: If value is not the appropriate type for valueType
	public convenience init(type valueType: PGType, value: Any, connection: Connection) throws {
		try self.init(type: valueType, value: value, datesAsIntegers: connection.hasIntegerDatetimes)
	}

	/// intenral constructor that passes the single dateAsInteger value instead of the whole connection
	internal init(type valueType: PGType, value: Any, datesAsIntegers: Bool) throws {
		// FIXME: this doesn't seem like the right way to do this, but can't call our own methods. Maybe something in NativeType?
		if valueType == .float, value is Float {
			throw PostgreSQLStatusErrors.floatsMustbeDoubles
		}
		self.valueType = valueType
		let dataType = valueType.nativeType.metaType()
		guard type(of: value) == dataType
			else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		(bytes, valueCount) = try BinaryUtilities.bytes(forValue: value, asType: valueType, datesAsIntegers: datesAsIntegers)
	}

	deinit {
		bytes.deallocate()
	}
}
