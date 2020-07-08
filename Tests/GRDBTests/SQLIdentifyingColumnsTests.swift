import XCTest
@testable import GRDB

class SQLIdentifyingColumnsTests: GRDBTestCase {
    func testIdentifyingColumns() throws {
        try makeDatabaseQueue().write { db in
            try db.create(table: "t") { t in
                t.autoIncrementedPrimaryKey("id")
            }
            
            let alias = TableAliasBase(tableName: "t")
            let otherAlias = TableAliasBase()
            
            let aliased_a = Column("a")._qualifiedExpression(with: alias)
            let aliased_b = Column("b")._qualifiedExpression(with: alias)
            let aliased_pk = _SQLExpressionFastPrimaryKey()._qualifiedExpression(with: alias)
            let otherAliased_a = Column("a")._qualifiedExpression(with: otherAlias)

            try XCTAssertEqual((aliased_a == 1).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((aliased_a === 1).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((aliased_a == nil).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((1 == aliased_a).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((aliased_pk == 1).identifyingColums(db, for: alias), ["id"])
            try XCTAssertEqual((aliased_a == 1 && aliased_a == 2).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((aliased_a == 1 && aliased_b == 1).identifyingColums(db, for: alias), ["a", "b"])
            try XCTAssertEqual((aliased_a == 1 && aliased_b > 1).identifyingColums(db, for: alias), ["a"])
            try XCTAssertEqual((otherAliased_a == 1 && aliased_b == 1).identifyingColums(db, for: alias), ["b"])
            
            try XCTAssertEqual((otherAliased_a).identifyingColums(db, for: alias), [])
            try XCTAssertEqual((aliased_a == 1 || aliased_a == 2).identifyingColums(db, for: alias), [])
            try XCTAssertEqual((aliased_a == aliased_b).identifyingColums(db, for: alias), [])
            try XCTAssertEqual((aliased_a > 1).identifyingColums(db, for: alias), [])
            try XCTAssertEqual((aliased_a != 1).identifyingColums(db, for: alias), [])
        }
    }
}
