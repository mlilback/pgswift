//
//  PGType.swift
//  pgswift
//
//  Created by Mark Lilback on 8/28/19.
//
// Copied from [Vapor](https://github.com/vapor-community/postgresql/)

import Foundation
import CLibpq

/// supported postgresql data types
public enum PGType: UInt32, CaseIterable {
	/// unsupported data types have a native data type of string and return a string represenation of the value, or an empty string
	case unsupported = 0
	
	/// uses native type of .bool
	case bool = 16
	
	/// uses native type of .int
	case int2 = 21
	/// uses native type of .int
	case int4 = 23
	/// uses native type of .int
	case int8 = 20
	/// uses native type of .double
	case float = 700
	/// uses native type of .double
	case double = 701
	/// not yet supported, but planned
	case numeric = 1700
	
	/// uses native type of .string
	case char = 18
	/// uses native type of .string
	case name = 19
	/// uses native type of .string
	case text = 25
	/// uses native type of .string
	case bpchar = 1042
	/// uses native type of .string
	case varchar = 1043
	/// uses native type of .string
	case json = 114
	/// uses native type of .string
	case xml = 142
	
	/// uses native type of .date
	case date = 1082
	/// uses native type of .date
	case time = 1083
	/// uses native type of .date
	case timetz = 1266
	/// uses native type of .date
	case timestamp = 1114
	/// uses native type of .date
	case timestamptz = 1184
	
	/// uses native type of .data
	case bytea = 17
	
	/// the native type (int, string, date) used for this PGtype
	public var nativeType: NativeType {
		switch self {
		case .unsupported:
			return .string
			
		case .bool:
			return .bool

		case .int2:
			fallthrough
		case .int4:
			fallthrough
		case .int8:
			return .int

		case .float:
			return .float
		case .double:
			return .double

		case .numeric:
			return .string

		case .char:
			fallthrough
		case .name:
			fallthrough
		case .text:
			fallthrough
		case .bpchar:
			fallthrough
		case .varchar:
			fallthrough
		case .json:
			fallthrough
		case .xml:
			return .string
		
		case .bytea:
			return .data
		
		case .date:
			fallthrough
		case .time:
			fallthrough
		case .timetz:
			fallthrough
		case .timestamp:
			fallthrough
		case .timestamptz:
			return .date
		}
	}
}

/// Swift types a database value can be returned as
public enum NativeType: String, CaseIterable {
	/// returned as *Int*
	case int
	/// returned as *Bool*
	case bool
	/// returned as *Double*
	case float
	/// returned as *Double*
	case double
	/// returned as *String*
	case string
	/// returned as *Date*
	case date
	/// returned as *Data*
	case data
	
	/// Test if the native type's class is the same as the type parameter
	///
	/// - Parameter type: the type to test this MativeType to
	/// - Returns: true if this NativeType is type
	public func matches(_ type: Any.Type) -> Bool {
		switch self {
			case .int: return Int.self == type
			case .bool: return Bool.self == type
			case .float: return Float.self == type
			case .double: return Double.self == type
			case .string: return String.self == type
			case .data: return Data.self == type
			case .date: return Date.self == type
		}
	}

	/// returns the metatype for this native type. For instance, .string returns String.self
	///
	/// - Returns: the metatype for this NativeType (type.self)
	public func metaType() -> Any.Type {
		switch self {
		case .int: return Int.self
		case .bool: return Bool.self
		case .float: return Double.self
		case .double: return Double.self
		case .string: return String.self
		case .data: return Data.self
		case .date: return Date.self
		}
	}

}
