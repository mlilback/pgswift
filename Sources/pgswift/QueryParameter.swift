//
//  QueryParameter.swift
//  pgswift
//
//  Created by Mark Lilback on 8/31/19.
//

import Foundation

public final class QueryParameter {
	public let valueType: PGType
	let valueBytes: UnsafePointer<Int8>
	let valueCount: Int
	
	public init(type valueType: PGType, value: Any, datesAsIntegers: Bool) throws {
		self.valueType = valueType
		let dataType = valueType.nativeType.metaType()
		guard type(of: value) == dataType
			else { throw PostgreSQLStatusErrors.unsupportedDataFormat }
		(valueBytes, valueCount) = try BinaryUtilities.bytes(forValue: value, asType: valueType, datesAsIntegers: datesAsIntegers)
	}
	
	deinit {
		valueBytes.deallocate()
	}
}
