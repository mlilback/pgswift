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

final class QueryTests: BaseTest {
	
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
			if !result.wasSuccessful {
				XCTFail("query failed: \(result.errorMessage)")
			}
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
			XCTFail("query failed: \(err)")
		} catch {
			XCTFail("unknown error")
		}
	}

	func testBinaryQuery() {
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		do {
			let result = try con.executeBinary(query: "select * from person")
			XCTAssertEqual(result.rowCount, 3)
			XCTAssertEqual(try result.getValue(row: 0, columnName: "name"), "mark")
			XCTAssertEqual(try result.getValue(row: 1, columnName: "age"), 44)
			XCTAssertNil(try result.getValue(row: 2, columnName: "age"))
			let str: String? = try result.getValue(row: 1, columnName: "name")
			XCTAssertNotNil(str)
			XCTAssertEqual(str!, "kenny")
			XCTAssertEqual(try result.getValue(row: 0, columnName: "member"), true)
			XCTAssertEqual(try result.getValue(row: 0, columnName: "fval")!, Double(2.34), accuracy: 0.00001)
			XCTAssertEqual(try result.getValue(row: 0, columnName: "dval")!, Double(0.000454), accuracy: 0.00001)
			let int2: Int = try result.getValue(row: 0, columnName: "smint")!
			XCTAssertEqual(int2, 3)
			
			let sdate: Date = try result.getValue(row: 0, columnName: "signupDate")!
			XCTAssertNotNil(sdate)
			let onlyDate = sdate.addingTimeInterval(12.0 * 60.0 * 60.0)
			XCTAssertEqual(dateFormatter!.string(from: onlyDate), "1/8/2019")
			
			let onlyTime: Date = try result.getValue(row: 0, columnName: "atime")!
			let timeStr = BinaryUtilities.DateTime.timeFormatter.string(from: onlyTime)
			XCTAssertEqual(timeStr, "11:31:21.0540") // formatter returns 4 digits, so add 0 on end
			
			let expectedBlob = "0a323621".data(using: .hexadecimal)!
			let rdata: Data = try result.getValue(row: 0, columnName: "thumb")!
			XCTAssertEqual(rdata, expectedBlob)
		} catch let err as PostgreSQLError {
			XCTFail(err.localizedDescription)
		} catch {
			XCTFail("unknown error: \(error)")
		}
	}

	// test smallint, integer
	func testParamQuery() {
		guard let con = connection else { XCTFail(); return }
		XCTAssert(con.isConnected)
		let query = "INSERT INTO person (id, name, age, member, fval, signupDate, dval, smint, atime, signupStamp, thumb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
		do {
			let signDate = dateFormatter!.date(from: "11-21-2018")!
			let signTime = BinaryUtilities.DateTime.timeFormatter.date(from: "14:34:21.2340")!
			let signStamp = BinaryUtilities.DateTime.timestampFormatter.date(from: "2011-09-22T07:37:21.456+00:00")!
			let blob = "0x0A456230".data(using: .hexadecimal)!
			let params: [QueryParameter?] = [
				try QueryParameter(type: .int8, value: 50, connection: con),
				try QueryParameter(type: .varchar, value: "Julia", connection: con),
				try QueryParameter(type: .int4, value: 24, connection: con),
				try QueryParameter(type: .bool, value: true, connection: con),
				try QueryParameter(type: .double, value: Double(1.2), connection: con),
				// test coverage for internal initializer
				try QueryParameter(type: .date, value: signDate, connection: con),
				try QueryParameter(type: .double, value: Double(0.032), datesAsIntegers: con.hasIntegerDatetimes),
				try QueryParameter(type: .int2, value: 33, connection: con),
				try QueryParameter(type: .time, value: signTime, connection: con),
				try QueryParameter(type: .timestamp, value: signStamp, connection: con),
				try QueryParameter(type: .bytea, value: blob, connection: con),
			]
			let result = try con.execute(query: query, parameters: params)
			if !result.wasSuccessful {
				XCTFail("insert failed with error: \(result.errorMessage)")
			}
			XCTAssertEqual(result.rowsAffected, "1")
			// TODO: select the row and make sure it was inserted properly
		} catch let err as PostgreSQLError {
			print(err.localizedDescription)
			XCTFail()
		} catch {
			XCTFail("unknown error: \(error)")
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

	override var initialSQL: String { return """
		CREATE TABLE person (
		id integer not null primary key,
		name varchar(20) not null,
		age int,
		signupDate date DEFAULT NOW(),
		signupStamp timestamp with time zone default now(),
		member boolean default false,
		fval float,
		dval double precision,
		smint smallint,
		atime time,
		thumb bytea);
		INSERT INTO person (id, name, age, signupDate, signupStamp, member, fval, dval, smint, atime, thumb) VALUES (1, 'mark', 46, '2019-01-08', '2019-08-30 03:13:15.607487+00', true, 2.34, 0.000454, 3, '11:31:21.054', '\\x0a323621');
		INSERT INTO person (id, name, age, signupDate) VALUES (2, 'kenny', 44, '2019-03-11');
		INSERT INTO person (id, name) VALUES (3, 'brinley');
		""" }
	
	override var cleanupSQL: String? { return "drop table person" }

	static var allTests = [
		("testBasicQuery", testBasicQuery),
		("testBinaryQuery", testBinaryQuery),
		("testNotifications", testNotifications),
	]
}
