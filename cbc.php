<?php

class DatabaseConnection {
    protected string $serverName;
    protected string $database;
    protected string $user;
    protected string $password;
    protected $conn;

    public function __construct(string $serverName, string $database, string $user, string $password) {
        $this->serverName = $serverName;
        $this->database = $database;
        $this->user = $user;
        $this->password = $password;
        $this->connect();
    }

    private function connect(): void {
        $this->conn = sqlsrv_connect($this->serverName, [
            "Database" => $this->database,
            "Uid" => $this->user,
            "PWD" => $this->password
        ]);

        if (!$this->conn) {
            die("❌ SQL Server Connection Failed: " . print_r(sqlsrv_errors(), true));
        }
    }

    public function getConnection() {
        return $this->conn;
    }

    public function closeConnection(): void {
        sqlsrv_close($this->conn);
    }
}

class MongoDBHandler {
    protected $manager;
    protected string $database;

    public function __construct(string $connectionString, string $database) {
        $this->database = $database;
        $this->manager = new MongoDB\Driver\Manager($connectionString);
    }

    public function fetchData(array $pipeline): array {
        $query = new MongoDB\Driver\Command([
            'aggregate' => 'labresults',
            'cursor' => new stdClass(),
            'allowDiskUse' => true,
            'pipeline' => $pipeline
        ]);
        
        try {
            $rows = $this->manager->executeCommand($this->database, $query);
            return iterator_to_array($rows);
        } catch (MongoDB\Driver\Exception\Exception $e) {
            echo "❌ MongoDB Query Error: " . $e->getMessage() . "\n";
            return [];
        }
    }
}

class DataProcessor {
    private array $data;

    public function __construct(array $data) {
        $this->data = $data;
    }

    public function process(): array {
        return array_map(fn($document) => (array) $document, $this->data);
    }
}

class SQLDataInserter {
    private $conn;

    public function __construct($conn) {
        $this->conn = $conn;
    }

    public function insertData(array $data): void {
        $sql = "INSERT INTO labresults_items_list (PatientMRN, visitid, ResultName, ResultValuesId, resultvalue, NormalRange, UOMDescription, HLN, ShortText, OrderNumber, OrderItemCode, OrderDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        foreach ($data as $d) {
            $params = array_values($d);
            $stmt = sqlsrv_query($this->conn, $sql, $params);
            
            if (!$stmt) {
                echo "❌ SQL Insert Failed: " . print_r(sqlsrv_errors(), true) . "\n";
            }
        }
    }
}

// Initialize objects dynamically
$mongo = new MongoDBHandler(getenv('MONGO_CONN_STRING'), getenv('MONGO_DB_NAME'));
$sqlDb = new DatabaseConnection(getenv('SQL_SERVER_NAME'), getenv('SQL_DATABASE'), getenv('SQL_USER'), getenv('SQL_PASSWORD'));

$pipeline = [
    ['$limit' => 100],
    ['$lookup' => ['from' => 'orderitems', 'localField' => 'orderitemuid', 'foreignField' => '_id', 'as' => 'orderitem']],
    ['$unwind' => '$orderitem'],
    ['$match' => ['orderitem.name' => 'COMPLETE BLOOD COUNT']],
    ['$unwind' => '$resultvalues'],
    ['$project' => ['patientmrn' => '$PatientMRN.mrn', 'visitid' => '$visitidDetails.visitid', 'resultvalue' => '$resultvalues.resultvalue']]
];

$mongoData = $mongo->fetchData($pipeline);
$processedData = (new DataProcessor($mongoData))->process();
$inserter = new SQLDataInserter($sqlDb->getConnection());
$inserter->insertData($processedData);

$sqlDb->closeConnection();

echo "✅ Data processing completed successfully!";

?>
