/// SQLQuery is a representation of an SQL query.
///
/// See SQLQueryGenerator for actual SQL generation.
struct SQLQuery {
    var relation: SQLRelation
    var isDistinct: Bool = false
    var groupPromise: DatabasePromise<[SQLExpression]>?
    // Having clause is an array of expressions that we'll join with
    // the AND operator. This gives nicer output in generated SQL:
    // `(a AND b AND c)` instead of `((a AND b) AND c)`.
    var havingExpressionsPromise: DatabasePromise<[SQLExpression]> = DatabasePromise(value: [])
    var limit: SQLLimit?
}

extension SQLQuery: Refinable {
    func distinct() -> Self {
        with(\.isDistinct, true)
    }
    
    func limit(_ limit: Int, offset: Int? = nil) -> Self {
        with(\.limit, SQLLimit(limit: limit, offset: offset))
    }
    
    func qualified(with alias: TableAlias) -> Self {
        // We do not qualify group and having clauses now. They will be
        // in SQLQueryGenerator.init()
        map(\.relation) { $0.qualified(with: alias) }
    }
}

extension SQLQuery: AggregatingRequest {
    func group(_ expressions: @escaping (Database) throws -> [SQLExpressible]) -> Self {
        with(\.groupPromise, DatabasePromise { db in try expressions(db).map(\.sqlExpression) })
    }
    
    func having(_ predicate: @escaping (Database) throws -> SQLExpressible) -> Self {
        map(\.havingExpressionsPromise) { havingExpressionsPromise in
            DatabasePromise { db in
                try havingExpressionsPromise.resolve(db) + [predicate(db).sqlExpression]
            }
        }
    }
}

extension SQLQuery {
    func fetchCount(_ db: Database) throws -> Int {
        try QueryInterfaceRequest<Int>(query: countQuery(db)).fetchOne(db)!
    }
    
    private func countQuery(_ db: Database) throws -> SQLQuery {
        guard groupPromise == nil && limit == nil else {
            // SELECT ... GROUP BY ...
            // SELECT ... LIMIT ...
            return trivialCountQuery
        }
        
        if relation.children.contains(where: { $0.value.impactsParentDefinition }) { // TODO: not tested
            // SELECT ... FROM ... JOIN ...
            return trivialCountQuery
        }
        
        guard case .table = relation.source else {
            // SELECT ... FROM (something which is not a plain table)
            return trivialCountQuery
        }
        
        let selection = try relation.selectionPromise.resolve(db)
        GRDBPrecondition(!selection.isEmpty, "Can't generate SQL with empty selection")
        if selection.count == 1 {
            guard let count = selection[0].count(distinct: isDistinct) else {
                return trivialCountQuery
            }
            var countQuery = map(\.relation) { $0.unordered() }
            countQuery.isDistinct = false
            switch count {
            case .all:
                countQuery = countQuery.map(\.relation) { $0.select(SQLExpressionCount(AllColumns())) }
            case .distinct(let expression):
                countQuery = countQuery.map(\.relation) { $0.select(SQLExpressionCountDistinct(expression)) }
            }
            return countQuery
        } else {
            // SELECT [DISTINCT] expr1, expr2, ... FROM tableName ...
            
            guard !isDistinct else {
                return trivialCountQuery
            }
            
            // SELECT expr1, expr2, ... FROM tableName ...
            // ->
            // SELECT COUNT(*) FROM tableName ...
            return map(\.relation) { $0.unordered().select(SQLExpressionCount(AllColumns())) }
        }
    }
    
    // SELECT COUNT(*) FROM (self)
    private var trivialCountQuery: SQLQuery {
        let relation = SQLRelation(
            source: .subquery(map(\.relation) { $0.unordered() }),
            selectionPromise: DatabasePromise(value: [SQLExpressionCount(AllColumns())]))
        return SQLQuery(relation: relation)
    }
}

struct SQLLimit {
    let limit: Int
    let offset: Int?
    
    var sql: String {
        if let offset = offset {
            return "\(limit) OFFSET \(offset)"
        } else {
            return "\(limit)"
        }
    }
}
