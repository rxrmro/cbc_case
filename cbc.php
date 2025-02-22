<?php

set_time_limit(20000);
 echo 'eee';

 
$mongoConn = "mongodb://csmcreport1:Arcu%24RPT%40cM%24c@10.12.0.23:45431/arcusairdb";
$mongodbname = "arcusairdb";

$myServer = "12.0.6024.0";
$myUser = "Integration";
$myPass = "!nt3gr@Tion";
$myDB = "CBCDB";
$serverName = "SRV-DASHBOARD01";

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
        [
            '$lookup' => [
                'from' => 'patients',
                'localField' => 'patientuid',
                'foreignField' => '_id',
                'as' => 'PatientMRN'
            ]
        ],
        ['$unwind' => '$PatientMRN'],
        [
            '$lookup' => [
                'from' => 'patientvisits',
                'localField' => 'patientvisituid',
                'foreignField' => '_id',
                'as' => 'visitidDetails'
            ]
        ],
        ['$unwind' => '$orderDetails'],
        //  Removed the duplicate unwind
        [
            '$project' => [
                'visitid' => '$visitidDetails.visitid',
                'patientmrn' => '$PatientMRN.mrn',
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

    try {
        $rows = $mongo->executeCommand($mongodbname, $query);
        //echo "<pre>MongoDB Results:\n";
        // print_r(iterator_to_array($rows));
        foreach ($rows as $document)
        {
          //  var_dump ($document);
            $data[] = [
                'PatientMRN' => $document->patientmrn ?? '',
                'visitid' => $document->visitid ?? '',
                'ResultName' => $document->resultName ?? '',
                'ResultValuesId' => ''.($document->resultvaluesId ?? ''),
                'resultvalue' => $document->resultvalue ?? '',
                'NormalRange' => $document->normalRange ?? '',
                'UOMDescription' => $document->uomDescription ?? '',
                'HLN' => $document->HLN ?? '',
                'ShortText' => $document->shorttext ?? '',
                'OrderNumber' => $document->ordernumber ?? '',
                'OrderItemCode' => $document->orderitemcode ?? '',
                'OrderDate' => $document->orderdate ?? '',
            ];
        }
        //echo "</pre>\n";
    } catch (MongoDB\Driver\Exception\Exception $mongoError) {
        echo "❌ MongoDB Query Error: " . $mongoError->getMessage() . "\n";
        $rows = null; // Ensure $rows is null if the query fails
    }
    // SQL Server Connection
    $connectionOptions = [
        "Database" => "PBDASHBOARDAA",
        "Uid" => "Integration",
        "PWD" => "!nt3gr@Tion"
    ];
    $conn = sqlsrv_connect($serverName, $connectionOptions);

    if ($conn === false) {
        echo "❌ SQL Server Connection Failed:\n";
      //  echo "<pre>"; print_r(sqlsrv_errors()); echo "</pre>\n";  // Print errors for debugging
         // Don't die, just set $conn to null to indicate failure
        $conn = null;
    } else {
        echo "✅ Connected to SQL Server successfully!\n";

          // Example SQL Server Query (replace with your actual query)
        $sql = "SELECT TOP 10 * FROM labresults_items_list"; // Replace SomeTable with your table name
        $stmt = sqlsrv_query($conn, $sql);

        if ($stmt === false) {
            echo "❌ SQL Server Query Failed:\n";
            echo "<pre>"; print_r(sqlsrv_errors()); echo "</pre>\n";
        } else {
              $sqlServerResults = [];
            while ($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)) {
                $sqlServerResults[] = $row;
            }

            echo "<pre>SQL Server Results:\n";
            //print_r($sqlServerResults);
            echo "</pre>\n";
        }
        if (isset($stmt)) {
            sqlsrv_free_stmt($stmt); // Free statement resource
        }
    }

} catch (Exception $e) {
    echo "❌ General Error: " . $e->getMessage() . "\n"; // Catch other potential exceptions
} finally {
    if (isset($conn)) {
        // sqlsrv_close($conn);     // Close connection
    }
}



function getData() {
    return [
        'PatientMRN' => $_POST['patientmrn'] ?? null,
        'visitid' => $_POST['visitid'] ?? null,
        'ResultName' => $_POST['resultname'] ?? null,
        'ResultValuesId' => $_POST['resultvaluesid'] ?? null,
        'resultvalue' => $_POST['resultvalue'] ?? null,
        'NormalRange' => $_POST['normalrange'] ?? null,
        'UOMDescription' => $_POST['uomdescription'] ?? null,
        'HLN' => $_POST['HLN'] ?? null,
        'ShortText' => $_POST['shorttext'] ?? null,
        'OrderNumber' => $_POST['ordernumber'] ?? null,
        'OrderItemCode' => $_POST['orderitemcode'] ?? null,
        'OrderDate' => $_POST['orderdate'] ?? null,
    ];
    // var_dump ($_POST);
}

// Check if form is submitted
    // $data = getData();

    // Prepare the SQL INSERT statement
    // var_dump ($sql);


    

      
            foreach ($data as $d)
{

    $sql = "INSERT INTO labresults_items_list (
        PatientMRN, visitid, ResultName, ResultValuesId, resultvalue, NormalRange, UOMDescription, 
        HLN, ShortText, OrderNumber, OrderItemCode, OrderDate

    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )";
    $params = array_values($d);
    // echo "XXXX<pre>";print_r($params);echo "</pre>";
   // var_dump($params);
    // Execute the query
    $stmt = sqlsrv_query($conn, $sql, $params);

    if ($stmt === false) {
        die(print_r(sqlsrv_errors(), true)); // Handle query execution errors
    } else {
        echo "Data inserted successfully!";
    }

}

    
  

    // Free statement and close connection
    sqlsrv_free_stmt($stmt);

// Close the database connection
sqlsrv_close($conn);


die();
?>



