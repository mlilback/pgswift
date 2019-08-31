//
//  QueryTests.swift
//  pgswift
//
//  Created by Mark Lilback on 8/29/19.
//

import Foundation
import XCTest
@testable import pgswift

// need to test: "insert returning"

extension Date {
	var onlyDate: Date {
		let calendar = Calendar.current
		var dateComps = calendar.dateComponents([.year,.month,.day], from: self)
		dateComps.timeZone = TimeZone.init(abbreviation: "UTC")!
		return calendar.date(from: dateComps)!
		
	}
}

final class QueryTests: XCTestCase {
	var connection: Connection?
	var dateFormatter: DateFormatter?
	var timestampFormatter = DateFormatter()
	
	override func setUp() {
		let basicInfo = ConnectInfo(host: "localhost", port: "5433", user: "test", password: "secret", dbname: "test", sslMode: .allow)
		dateFormatter = DateFormatter()
		dateFormatter?.locale = Locale(identifier: "en_US_POSIX")
//		dateFormatter?.dateStyle = .short
//		dateFormatter?.timeStyle = .none
		dateFormatter?.setLocalizedDateFormatFromTemplate("M/d/yyyy")
//		dateFormatter?.timeZone = TimeZone.current
		timestampFormatter.dateFormat = " yyyy-MM-dd HH:m:s.SSSSx"
		timestampFormatter.locale = Locale(identifier: "en_US_POSIX")
		timestampFormatter.timeZone = TimeZone(secondsFromGMT: 0)

		connection = Connection(connectInfo: basicInfo)
		try! connection?.open()
		XCTAssert(connection?.isConnected ?? false)
		let result = try! connection?.execute(query: """
			CREATE TABLE person (
				id integer not null primary key,
				name varchar(20) not null,
				age int,
				signupDate date DEFAULT NOW(),
				signupStamp timestamp with time zone default now(),
				member boolean default false,
				fval float,
				dval double precision);
			INSERT INTO person (id, name, age, signupDate, signupStamp, member, fval, dval) VALUES (1, 'mark', 46, '2019-01-08', '2019-08-30 03:13:15.607487+00', true, 2.34, 0.000454);
			INSERT INTO person (id, name, age, signupDate) VALUES (2, 'kenny', 44, '2019-03-11');
			INSERT INTO person (id, name) VALUES (3, 'brinley');
		""")
		let status = result!.status
		print("stat=\(status), lastError=\(result!.errorMessage)")
		XCTAssert(result!.wasSuccessful)
	}
	
	override func tearDown() {
		try! connection?.execute(query: "drop table person")
		connection?.close()
	}
	
	func testBasicQuery() {
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		do {
			// provide code coverage
			try con.validateConnection()
			let sversion = try con.serverVersion()
			XCTAssertTrue(sversion.starts(with: "9.6."))
			
			// select all persons
			let result = try con.execute(query: "select id, name, age, signupDate, signupStamp, member, fval, dval from person")
			XCTAssertEqual(result.rowCount, 3)
			XCTAssertEqual(try result.getStringValue(row: 0, column: 1), "mark")
			XCTAssertEqual(try result.getIntValue(row: 1, column: 2), 44)
			XCTAssertNil(try result.getIntValue(row: 2, column: 2))
			XCTAssertTrue(try result.getBoolValue(row: 0, column: 5)!)
			XCTAssertEqual(try result.getFloatValue(row: 0, column: 6)!, 2.34, accuracy: 0.01)
			XCTAssertEqual(try result.getDoubleValue(row: 0, column: 7)!, 0.000454, accuracy: 0.00001)
			let nameByName: String? = try result.getValue(row: 0, columnName: "name")
			XCTAssertNotNil(nameByName)
			XCTAssertEqual(nameByName!, "mark")
//			let signup = try result.getDateValue(row: 0, column: 4)!
//			print("signup = \(String(describing: signup))")
			XCTAssertEqual(con.lastErrorMessage, "")
		} catch let err as PostgreSQLError {
			print(err.localizedDescription)
			XCTFail()
		} catch {
			XCTFail("unknown error")
		}
	}

	func testBinaryQuery() {
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		do {
			let result = try con.executeBinary(query: "select id, name, age, signupDate, signupStamp from person")
			XCTAssertEqual(result.rowCount, 3)
			XCTAssertEqual(try result.getStringValue(row: 0, column: 1), "mark")
			XCTAssertEqual(try result.getIntValue(row: 1, column: 2), 44)
			XCTAssertNil(try result.getIntValue(row: 2, column: 2))
			let sdate = try result.getDateValue(row: 0, column: 3)
			XCTAssertNotNil(sdate)
			let onlyDate = sdate!.addingTimeInterval(12.0 * 60.0 * 60.0)
			XCTAssertEqual(dateFormatter!.string(from: onlyDate), "1/8/2019")
			// not sure how to test timestamp compared to string. dateFormatter with proper format string keep returning nil
//			let signup = try result.getDateValue(row: 0, column: 4)!
//			let signupStr = timestampFormatter.string(from: signup)
//			XCTAssertEqual(signupStr, "2019-08-30 03:13:15.607487+00")
//			print("signup = \(signupStr)")
		} catch let err as PostgreSQLError {
			XCTFail(err.localizedDescription)
		} catch {
			XCTFail("unknown error")
		}
	}
	
	func testNotifications() {
		let channelName = "foobar"
		let payload = "barfoo"
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		let expectation = self.expectation(description: "notification delivery")
		var theNote: PGNotification?
		var theError: Error?
		do {
			let source = try con.listen(toChannel: channelName, queue: .global()) { (note, error) in
				theNote = note
				theError = error
				expectation.fulfill()
			}
			source.resume()
			sleep(1)
			
			let nresult = try con.execute(query: "select pg_notify('\(channelName)', '\(payload)');")
			if !nresult.wasSuccessful {
				XCTFail("failed to post notification")
			}
			
			waitForExpectations(timeout: 3) { err in
				if err != nil { XCTFail("notification timeout") }
			}
			XCTAssertNil(theError)
			guard let note = theNote else { XCTFail(); return }
			XCTAssertNotNil(theNote)
			XCTAssertEqual(note.channel, channelName)
			XCTAssertEqual(note.payload, payload)
		} catch let err as PostgreSQLError {
			XCTFail(err.localizedDescription)
		} catch {
			XCTFail("unknown error")
		}
	}
	
	static var allTests = [
		("testBasicQuery", testBasicQuery),
		("testBinaryQuery", testBinaryQuery),
		("testNotifications", testNotifications),
	]
}
