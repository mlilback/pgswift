import XCTest
@testable import pgswift

final class ConnectParamTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(pgswift().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
