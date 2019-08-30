import XCTest
@testable import pgswift

final class ConnectParamTests: XCTestCase {
	var pg: Connection?
	
	override func tearDown() {
		pg?.close()
	}
	
	func testBasicConnect() {
		let basicInfo = ConnectInfo(host: "localhost", port: "5433", user: "test", password: "secret", dbname: "test", sslMode: .allow)
		pg = Connection(connectInfo: basicInfo)
		try! pg?.open()
		XCTAssert(pg?.isConnected ?? false)
	}

	static var allTests = [
		("testBasicConnect", testBasicConnect),
	]
}
