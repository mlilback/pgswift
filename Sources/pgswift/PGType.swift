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
	
	case bool = 16
	
	case int2 = 21
	case int4 = 23
	case int8 = 20
	case float = 700
	case double = 701
	case numeric = 1700
	
	case char = 18
	case name = 19
	case text = 25
	case bpchar = 1042
	case varchar = 1043
	case json = 114
	case xml = 142
	
	case date = 1082
	case time = 1083
	case timetz = 1266
	case timestamp = 1114
	case timestamptz = 1184
	
	case bytea = 17
	
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
	case int
	case bool
	case float
	case double
	case string
	case date
	case data
}
