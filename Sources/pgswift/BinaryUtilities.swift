//
//  BinaryUtilities.swift
//  pgswift
//
//  Created by Mark Lilback on 8/30/19.
//
// laregely based on file with same name from [Vapor](https://github.com/vapor-community/postgresql/)

import Foundation

struct BinaryUtilities {
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

	static func parseFloat64(value: UnsafePointer<UInt8>) -> Double {
		let uintValue = value.withMemoryRebound(to: UInt64.self, capacity: 1) { ptr in
			return ptr.pointee
		}
		return Double(bitPattern: UInt64(bigEndian: uintValue))
	}
}
