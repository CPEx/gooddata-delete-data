<?php

try {
    $dataDir = getenv('KBC_DATADIR') . DIRECTORY_SEPARATOR;
    $configFile = $dataDir . 'config.json';
    $config = json_decode(file_get_contents($configFile), true);

    if (!isset($config['parameters']) || !$config['parameters']) {
        echo 'Missing config parameters';
        exit(1);
    }

    if (!isset($config['parameters']['goodDataWriterId'])) {
        echo 'Missing goodData writer ID';
        exit(1);
    }

    if (!isset($config['parameters']['goodDataProjectID'])) {
        echo 'Missing goodData project ID';
        exit(1);
    }

    if (!isset($config['parameters']['fromTable'])) {
        echo 'Missing "from table" value';
        exit(1);
    }

    if (!isset($config['parameters']['whereColumn'])) {
        echo 'Missing "Where column" value';
        exit(1);
    }

    if (!isset($config['parameters']['operator'])) {
        $config['parameters']['operator'] = '=';
    }

    if (!isset($config['parameters']['storageApiToken'])) {
        echo 'Missing storage Api Token';
        exit(1);
    }


    if (!isset($config['parameters']['whereValue'])) {
        if (isset($config['parameters']['daysAgo'])) {
            $config['parameters']['whereValue'] = date('Y-m-d', time() - intval($config['parameters']['daysAgo']) * 86400);
        } else {
            echo 'Missing "Where value" value';
            exit(1);
        }
    }

    $postDataObject = [
        "uri" => "/gdc/md/" . $config['parameters']['goodDataProjectID'] . "/dml/manage",
        'payload' => [
            "manage" => [
                "maql" =>
                    "DELETE FROM {" . $config['parameters']['fromTable'] . "} "
                    . "WHERE {" . $config['parameters']['whereColumn'] . "} "
                    . $config['parameters']['operator'] .
                    " \"" . $config['parameters']['whereValue'] . "\";"
            ]
        ]
    ];
    $data_string = json_encode($postDataObject);

    $url = "https://syrup.keboola.com/gooddata-writer/v2/" . $config['parameters']['goodDataWriterId'] . "/proxy";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data_string);

    curl_setopt($ch, CURLOPT_HTTPHEADER, array(
        'X-StorageApi-Token: ' . $config['parameters']['storageApiToken'],
        'Content-Type: application/json',
        'Content-Length: ' . strlen($data_string)
    ));


    $result = curl_exec($ch);
    $result = json_decode($result, true);
    $error = curl_error($ch);

    $statusCode = 200;
    try {
        $statusCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    } catch (Exception $exception) {
        $error .= ' - ' . $exception;
    }

    curl_close($ch);
    if ($statusCode < 200 || $statusCode >= 400) {
        echo $error;
        exit(2);
    } else {

        exit(0);
    }
} catch (InvalidArgumentException $e) {
    echo $e->getMessage();
    exit(1);
} catch (\Throwable $e) {
    echo $e->getMessage();
    exit(2);
}