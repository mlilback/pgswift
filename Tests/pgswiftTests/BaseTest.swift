//
//  BaseTest.swift
//  pgswiftTests
//
//  Created by Mark Lilback on 9/2/19.
//

import Foundation
import XCTest
@testable import pgswift

class BaseTest: XCTestCase {
	var connection: Connection?
	var dateFormatter: DateFormatter?
	var timestampFormatter = DateFormatter()
	
	override func setUp() {
		let basicInfo = ConnectInfo(host: "localhost", port: "5433", user: "test", password: "secret", dbname: "test", sslMode: .allow)
		dateFormatter = DateFormatter()
		dateFormatter?.locale = Locale(identifier: "en_US_POSIX")
		dateFormatter?.setLocalizedDateFormatFromTemplate("M/d/yyyy")
		timestampFormatter.dateFormat = "yyyy-MM-dd HH:m:s.SSSSx"
		timestampFormatter.locale = Locale(identifier: "en_US_POSIX")
		timestampFormatter.timeZone = TimeZone(secondsFromGMT: 0)
		
		connection = Connection(connectInfo: basicInfo)
		try! connection?.open()
		XCTAssert(connection?.isConnected ?? false)
		let result = try! connection?.execute(query: initialSQL)
		XCTAssert(result!.wasSuccessful)
	}
	
	override func tearDown() {
		if let sql = cleanupSQL {
			try! connection?.execute(query: sql)
		}
		connection?.close()
	}
	
	var initialSQL: String { return "" }
	var cleanupSQL: String? { return nil }

}
