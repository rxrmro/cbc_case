<?php

set_time_limit(20000);
echo 'Script started...';

// Securely fetch database credentials from environment variables
$mongoConn = getenv('MONGO_CONN_STRING');
$mongodbname = getenv('MONGO_DB_NAME');
$serverName = getenv('SQL_SERVER_NAME');
$sqlDatabase = getenv('SQL_DATABASE');
$sqlUser = getenv('SQL_USER');
$sqlPassword = getenv('SQL_PASSWORD');

$data = [];
try {
    // MongoDB Connection
    $mongo = new MongoDB\Driver\Manager($mongoConn);
    echo "✅ Connected to MongoDB successfully!\n";

    $pipeline = [
        ['$limit' => 100],
        [
            '$lookup' => [
                'from' => 'orderitems',
                'localField' => 'orderitemuid',
                'foreignField' => '_id',
                'as' => 'orderitem'
            ]
        ],
        ['$unwind' => '$orderitem'],
        [
            '$match' => [
                'orderitem.name' => 'COMPLETE BLOOD COUNT'
            ]
        ],
        ['$unwind' => '$resultvalues'],
        [
            '$lookup' => [
                'from' => 'patientorders',
                'localField' => 'patientorderuid',
                'foreignField' => '_id',
                'as' => 'orderDetails'
            ]
        ],
        ['$unwind' => '$orderDetails'],
        [
            '$project' => [
                'visitid' => '$orderDetails.visitid',
                'patientmrn' => '$orderDetails.patientmrn',
                'resultvaluesId' => '$resultvalues._id',
                'resultName' => '$resultvalues.name',
                'resultvalue' => '$resultvalues.resultvalue',
                'normalRange' => '$resultvalues.normalrange',
                'uomDescription' => '$resultvalues.uomdescription',
                'HLN' => '$resultvalues.HLN',
                'shorttext' => '$resultvalues.shorttext',
                'ordernumber' => '$orderDetails.ordernumber',
                'orderitemcode' => '$orderitem.code',
                'orderdate' => [
                    '$dateToString' => [
                        'format' => '%Y-%m-%d %H:%M:%S',
                        'timezone' => '+08:00',
                        'date' => '$approvaldate'
                    ]
                ]
            ]
        ]
    ];

    $query = new MongoDB\Driver\Command([
        'aggregate' => 'labresults',
        'cursor' => new stdClass,
        'allowDiskUse' => true,
        'pipeline' => $pipeline
    ]);

    $rows = $mongo->executeCommand($mongodbname, $query);
    foreach ($rows as $document) {
        $data[] = (array) $document;
    }
} catch (Exception $e) {
    echo "❌ MongoDB Error: " . $e->getMessage() . "\n";
}

// SQL Server Connection
$connectionOptions = [
    "Database" => $sqlDatabase,
    "Uid" => $sqlUser,
    "PWD" => $sqlPassword
];
$conn = sqlsrv_connect($serverName, $connectionOptions);

if (!$conn) {
    die("❌ SQL Server Connection Failed: " . print_r(sqlsrv_errors(), true));
}

echo "✅ Connected to SQL Server successfully!\n";

// Insert MongoDB data into SQL Server
foreach ($data as $d) {
    $sql = "INSERT INTO labresults_items_list (
        PatientMRN, visitid, ResultName, ResultValuesId, resultvalue, NormalRange, UOMDescription, 
        HLN, ShortText, OrderNumber, OrderItemCode, OrderDate
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    $params = array_values($d);
    $stmt = sqlsrv_query($conn, $sql, $params);
    if (!$stmt) {
        echo "❌ Insert Failed: " . print_r(sqlsrv_errors(), true);
    }
}

echo "Data inserted successfully!\n";
sqlsrv_close($conn);
?>
