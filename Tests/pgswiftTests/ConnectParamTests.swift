import XCTest
@testable import pgswift

final class ConnectParamTests: XCTestCase {
	var pg: Connection?
	
	override func tearDown() {
		pg?.close()
	}
	
	func testBasicParams() {
		let basicInfo = ConnectInfo(host: "localhost", port: "5433", user: "test", password: "secret", dbname: "test", sslMode: .allow)
//		basicInfo.withParamterCStrings { (nameStrings, valueStrings) in
//		}
		pg = Connection(connectInfo: basicInfo)
		try! pg?.open()
		XCTAssertNotNil(pg?.pgConnection)
		XCTAssert(pg?.isConnected ?? false)
	}

	static var allTests = [
		("testBasicParams", testBasicParams),
	]
}
