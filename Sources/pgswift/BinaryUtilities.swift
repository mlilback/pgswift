//
//  BinaryUtilities.swift
//  pgswift
//
//  Created by Mark Lilback on 8/30/19.
//
// laregely based on file with same name from [Vapor](https://github.com/vapor-community/postgresql/)

import Foundation

extension Float {
	var bigEndian: Float {
		return Float(bitPattern: bitPattern.bigEndian)
	}
}

extension Double {
	var bigEndian: Double {
		return Double(bitPattern: bitPattern.bigEndian)
	}
}

struct BinaryUtilities {
	enum DateTime {
		static let secondsInDay: Int32 = 24 * 60 * 60
		// Reference date in Postgres is 2000-01-01, while in Swift it is 2001-01-01. There were 366 days in the year 2000.
		static let timeIntervalBetween1970AndPostgresReferenceDate = Date.timeIntervalBetween1970AndReferenceDate - TimeInterval(366 * 24 * 60 * 60)
		static let referenceDate = Date(timeIntervalSince1970: timeIntervalBetween1970AndPostgresReferenceDate)
	}
	
	static func convert<T>(_ value: UnsafeMutablePointer<Int8>) -> T {
		return value.withMemoryRebound(to: T.self, capacity: 1) {
			$0.pointee
		}
	}

	/// The returned Pointer is owned by the caller and needs .dealloc() called
	static func valueToBytes<T>(_ value: inout T) -> (UnsafeMutablePointer<Int8>, Int) {
		let size = MemoryLayout.size(ofValue: value)
		return withUnsafePointer(to: &value) { valuePointer in
			return valuePointer.withMemoryRebound(to: Int8.self, capacity: size) { bytePointer in
				let bytes: UnsafeMutablePointer<Int8> = UnsafeMutablePointer.allocate(capacity: size)
				bytes.assign(from: bytePointer, count: size)
				return (bytes, size)
			}
		}
	}

	/// Returns bytes to use as binary input for the specified value
	/// The returned pointer is owned by the caller and needs .dealloc() called
	///
	/// - Parameters:
	///   - value: An object that is one of the NativeTypes
	///   - ofType: the type of value
	/// - Returns: tuple of the bytes and the length of the bytes.
	/// - Throws: if an unsupport data type
	public static func bytes(forValue value: Any, asType: PGType, datesAsIntegers: Bool = true) throws -> (UnsafePointer<Int8>, Int) {
		switch value {
		case is Bool:
			let bool = value as! Bool
			let bytes = UnsafeMutablePointer<Int8>.allocate(capacity: 1)
			bytes.initialize(to: bool ? 1 : 0)
			return (UnsafePointer(bytes), 1)
		case is Int:
			// integers are handled based on the desired type (2,4,8 byte)
			let intValue = (value as! Int).bigEndian
			switch asType {
			case .int2:
				var i2 = Int16(intValue)
				let (i2val, i2len) =  valueToBytes(&i2)
				return (UnsafePointer<Int8>(i2val), i2len)
			case .int4:
				var i4 = Int32(intValue)
				let (i4val, i4len) =  valueToBytes(&i4)
				return (UnsafePointer<Int8>(i4val), i4len)
			case .int8:
				var intVal = intValue
				let (ival, ilen) = valueToBytes(&intVal)
				return (UnsafePointer<Int8>(ival), ilen)
			default: fatalError()
			}
		case is Float:
			var orig = (value as! Float).bigEndian
			let (dval, dlen) = valueToBytes(&orig)
			return (UnsafePointer<Int8>(dval), dlen)
		case is Double:
			var origDouble = (value as! Double).bigEndian
			let (dval, dlen) = valueToBytes(&origDouble)
			return (UnsafePointer<Int8>(dval), dlen)
		case is String:
			let str = value as! String
			let data = UnsafeMutablePointer<Int8>.allocate(capacity: str.count)
			str.utf8CString.withUnsafeBytes { rawBuffer in
				let bufferPtr = rawBuffer.bindMemory(to: Int8.self)
				data.initialize(from: bufferPtr.baseAddress!, count: str.count)
			}
			return (UnsafePointer<Int8>(str), str.count) // is this null terminated?
		case is Data:
			// what a pain in the ass to what used to just be a call to .bytes()
			let data = value as! Data
			let bytes = UnsafeMutablePointer<UInt8>.allocate(capacity: data.count)
			data.copyBytes(to: bytes, count: data.count)
			let realBytes = UnsafeMutableRawPointer(bytes).bindMemory(to: Int8.self, capacity: data.count)
			return (UnsafePointer(realBytes), data.count)
		case is Date:
			return try dateToPointer(date: value as! Date, type: asType, asIntegers: datesAsIntegers)
		default:
			throw PostgreSQLStatusErrors.unsupportedDataFormat
		}
	
	}
	
	static func dateToPointer(date: Date, type: PGType, asIntegers: Bool) throws -> (UnsafePointer<Int8>, Int) {
		let cal = Calendar.current
		var dateVal = date
		if type == .date {
			// strip out time components
			let dateComps = cal.dateComponents([.year, .month, .day], from: date)
			dateVal = cal.date(from: dateComps)!
		}
		let interval = dateVal.timeIntervalSince(BinaryUtilities.DateTime.referenceDate)
		if asIntegers {
			let micro = Int64(interval * 1_000_000)
			var value = micro.bigEndian
			let (bytes, len) = valueToBytes(&value)
			return (UnsafePointer<Int8>(bytes), len)
		} else { // as float
			let seconds = Double(interval)
			var value = seconds.bigEndian
			let (bytes, len) = valueToBytes(&value)
			return (UnsafePointer<Int8>(bytes), len)
		}
	}
	
	static func parseFloat64(value: UnsafePointer<UInt8>) -> Double {
		let uintValue = value.withMemoryRebound(to: UInt64.self, capacity: 1) { ptr in
			return ptr.pointee
		}
		return Double(bitPattern: UInt64(bigEndian: uintValue))
	}
}
