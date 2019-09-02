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

extension String {
	enum ExtendedEncoding {
		case hexadecimal
	}
	
	/// If this string is valid hex code (with optional 0x prefix), returns a Data representation
	///
	/// - Parameter encoding: self's format. .hexadecimal for now
	/// - Returns: returns the converted Data, or nil if not valid hex
	func data(using encoding:ExtendedEncoding) -> Data? {
		let hexStr = self.dropFirst(self.hasPrefix("0x") ? 2 : 0)
		
		guard hexStr.count % 2 == 0 else { return nil }
		
		var newData = Data(capacity: hexStr.count/2)
		
		var indexIsEven = true
		for i in hexStr.indices {
			if indexIsEven {
				let byteRange = i...hexStr.index(after: i)
				guard let byte = UInt8(hexStr[byteRange], radix: 16) else { return nil }
				newData.append(byte)
			}
			indexIsEven.toggle()
		}
		return newData
	}
}

/// static methods for working with binary data
struct BinaryUtilities {

	/// static properties related to dates and times working with PostgreSQL
	enum DateTime {
		static let secondsInDay: Int32 = 24 * 60 * 60
		// Reference date in Postgres is 2000-01-01, while in Swift it is 2001-01-01. There were 366 days in the year 2000.
		static let timeIntervalBetween1970AndPostgresReferenceDate = Date.timeIntervalBetween1970AndReferenceDate - TimeInterval(366 * 24 * 60 * 60)
		static let referenceDate = Date(timeIntervalSince1970: timeIntervalBetween1970AndPostgresReferenceDate)

		/// a DateFormatter using 'YYYY-MM-DD' as its format
		static let dateFormatter: ISO8601DateFormatter = {
			var fmt = ISO8601DateFormatter()
			fmt.formatOptions = [.withFullDate, .withDashSeparatorInDate]
			return fmt
		}()

		/// a DateFormatter using 'HH:MM:SS.SSS' as its format
		static let timeFormatter: DateFormatter = {
			var fmt = DateFormatter()
			fmt.locale = Locale(identifier: "en_US_POSIX")
			fmt.dateFormat = "HH:mm:ss.SSSS"
			return fmt
		}()
		
		/// a Timestamp formatter
		static let timestampFormatter: ISO8601DateFormatter = {
			var fmt = ISO8601DateFormatter()
			fmt.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
			return fmt
		}()
	}
	
	/// converts a value to binary data
	/// The returned pointer is owned by the caller and needs .dealloc() called
	///
	/// - Parameter value: the value to convert
	/// - Returns: a mutable pointer and the current length
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
	/// The binary format for .date types is string
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
			let intValue = value as! Int
			switch asType {
			case .int2:
				var i2 = Int16(intValue).bigEndian
				let (i2val, i2len) =  valueToBytes(&i2)
				return (UnsafePointer<Int8>(i2val), i2len)
			case .int4:
				var i4 = Int32(intValue).bigEndian
				let (i4val, i4len) =  valueToBytes(&i4)
				return (UnsafePointer<Int8>(i4val), i4len)
			case .int8:
				var intVal = intValue.bigEndian
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
			return stringToPointer(str)
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
	
	/// Converts a date object to data to bind to a query
	///
	/// - Parameters:
	///   - date: the date
	///   - type: the type of the column this type will be inserted into
	///   - asIntegers: true if dates are stored as integers
	/// - Returns: the pointer and its length
	static func dateToPointer(date: Date, type: PGType, asIntegers: Bool) throws -> (UnsafePointer<Int8>, Int) {
		switch type {
		case .date:
			// use strings for date values
			let str = DateTime.dateFormatter.string(from: date)
			let (data, len) = stringToPointer(str)
			return (data, len + 1) // add null terminator
		case .timetz, .time:
			let str = DateTime.timeFormatter.string(from: date)
			let (data, len) = stringToPointer(str)
			return (data, len + 1)
		case .timestamp, .timestamptz:
			let str = DateTime.timestampFormatter.string(from: date)
			let (data, len) = stringToPointer(str)
			return (data, len + 1)
		default:
			throw PostgreSQLStatusErrors.unsupportedDataFormat
		}
//		let interval = date.timeIntervalSince(BinaryUtilities.DateTime.referenceDate)
//		if asIntegers {
//			let micro = Int64(interval * 1_000_000)
//			var value = micro.bigEndian
//			let (bytes, len) = valueToBytes(&value)
//			return (UnsafePointer<Int8>(bytes), len)
//		} else { // as float
//			let seconds = Double(interval)
//			var value = seconds.bigEndian
//			let (bytes, len) = valueToBytes(&value)
//			return (UnsafePointer<Int8>(bytes), len)
//		}
	}
	
	/// converts a string to a pointer
	///
	/// - Parameter str: the string to convert
	/// - Returns: the pointer and its length
	static func stringToPointer(_ str: String) -> (UnsafePointer<Int8>, Int) {
		// add on a extra byte for null terminator
		let data = UnsafeMutablePointer<Int8>.allocate(capacity: str.count + 1)
		str.utf8CString.withUnsafeBytes { rawBuffer in
			let bufferPtr = rawBuffer.bindMemory(to: Int8.self)
			data.initialize(from: bufferPtr.baseAddress!, count: str.count + 1)
		}
		return (UnsafePointer<Int8>(data), str.count)

	}
	
	/// parses a pointer containg a Double value
	///
	/// - Parameter value: pointer to a double value
	/// - Returns: the parsed double
	static func parseDouble(value: UnsafePointer<UInt8>) -> Double {
		let uintValue = value.withMemoryRebound(to: UInt64.self, capacity: 1) { ptr in
			return ptr.pointee
		}
		return Double(bitPattern: UInt64(bigEndian: uintValue))
	}
}
