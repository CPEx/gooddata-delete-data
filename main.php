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
        'Content-Length: ' . strlen($data_string),
        'User-Agent: CPEX-remove-old-data/0.2'
    ));

    function endProgram($with_error = false, $error = null) {
        if ($with_error) {
            echo $error;
            exit(2);
        } else {
            exit(0);
        }
    }

    function getStatus($url, $config) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, array(
            'X-StorageApi-Token: ' . $config['parameters']['storageApiToken'],
            'User-Agent: CPEX-remove-old-data/0.2'
        ));

        $result = curl_exec($ch);
        curl_close($ch);

        if ($result === false) {
            try {
                endProgram(true, curl_error($ch));
            } catch (Exception $exception) {
                endProgram(true, $exception->getMessage());
            }
        } else {
            try {
                $result = json_decode($result, true);
                if (isset($result['status']) && $result['status'] == "processing") {
                    sleep(5);
                    getStatus($url, $config);
                } elseif (isset($result['status']) && $result['status'] == "success") {
                    endProgram(false);
                } else {
                    if (isset($result['status'])) {
                        endProgram(true, 'Unknown status: '. $result['status']);
                    } else {
                        endProgram(true, 'Result url without status');
                    }
                }
            } catch (Exception $exception) {
                endProgram(true, $exception->getMessage());
            }
        }
    }


    $result = curl_exec($ch);

    if ($result === false) {
        try {
            endProgram(true, curl_error($ch));
            curl_close($ch);
        } catch (Exception $exception) {
            curl_close($ch);
            endProgram(true, $exception->getMessage());
        }
    } else {
        curl_close($ch);
        $result = json_decode($result, true);

        if (isset($result['url']) && $result['url'] != "") {
            getStatus($result['url'], $config);
        } else {
            if (isset($result['status'])) {
                if($result['status'] == 'waiting') {
                    endProgram(true, 'There is no URL with status');
                } else {
                    endProgram(true, 'Status: '.$result['status']);
                }
            } else {
                endProgram(true, 'There is not response status');
            }
        }
    }


} catch (InvalidArgumentException $e) {
    echo $e->getMessage();
    exit(1);
} catch (\Throwable $e) {
    echo $e->getMessage();
    exit(2);
}