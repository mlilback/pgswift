import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(ConnectParamTests.allTests),
		testCase(QueryTests.allTests),
    ]
}
#endif
