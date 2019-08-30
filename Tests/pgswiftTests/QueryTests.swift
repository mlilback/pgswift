//
//  QueryTests.swift
//  pgswift
//
//  Created by Mark Lilback on 8/29/19.
//

import Foundation
import XCTest
@testable import pgswift

final class QueryTests: XCTestCase {
	var connection: Connection?
	
	override func setUp() {
		let basicInfo = ConnectInfo(host: "localhost", port: "5433", user: "test", password: "secret", dbname: "test", sslMode: .allow)
		connection = Connection(connectInfo: basicInfo)
		try! connection?.open()
		XCTAssert(connection?.isConnected ?? false)
		let result = try! connection?.execute(query: """
			CREATE TABLE person (
				id integer not null primary key,
				name varchar(20) not null,
				age int,
				signupDate date DEFAULT NOW(),
				signupStamp timestamp with time zone default now());
			INSERT INTO person (id, name, age, signupDate) VALUES (1, 'mark', 46, '2019-01-08');
			INSERT INTO person (id, name, age, signupDate) VALUES (2, 'kenny', 44, '2019-03-11');
			INSERT INTO person (id, name) VALUES (3, 'brinley');
		""")
		print("STATUS = \(result!.status): \(connection?.lastErrorMessage  ?? "??")")
		XCTAssert(result!.status == .commandOk)
	}
	
	override func tearDown() {
		try! connection?.execute(query: "drop table person")
		connection?.close()
	}
	
	func testBasicQuery() {
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		do {
			let result = try con.execute(query: "select id, name, age, signupDate, signupStamp from person")
			XCTAssertEqual(result.rowCount, 3)
			XCTAssertEqual(try result.getStringValue(row: 0, column: 1), "mark")
			XCTAssertEqual(try result.getIntValue(row: 1, column: 2), 44)
			XCTAssertNil(try result.getIntValue(row: 2, column: 2))
			let signup = try result.getDateValue(row: 0, column: 4)
			print("signup = \(String(describing: signup))")
		} catch let err as PostgreSQLError {
			print(err.localizedDescription)
			XCTFail()
		} catch {
			XCTFail("unknown error")
		}
	}
	
	static var allTests = [
		("testBasicQuery", testBasicQuery),
	]
}
