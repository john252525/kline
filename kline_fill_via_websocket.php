<?php

require_once __DIR__ . '/../composer/vendor/autoload.php';


require_once __DIR__ . '/../.env.kline.php';  // $db_opts, BINANCE_SPOT_KLINE_TBL_PREFIX
require_once __DIR__.'/kline_functions.php';
$db = new SafeMySQL($db_opts);




$client = new \Binance\Websocket\Spot();

$callbacks = [
    'message' => function ($conn, $msg) {
      //echo $msg.PHP_EOL;

        $a = kline_convert_from_websocket_to_db($msg);

        $tbl = $a['pair'];

        kline_insert_into_db($tbl, $a);
    },
    'ping' => function ($conn, $msg) {
        if(VERBOSE) echo date('Y-m-d H:i:s')." received ping from server".PHP_EOL;
    }
];








$pairs_filename = __DIR__.'/'.'kline_pairs';
$pairs_url = 'https://';
$r = '';
if(is_file($pairs_filename)) $r = file_get_contents($pairs_filename);
if(empty($r)){
    $r = file_get_contents($pairs_url);
    if(!empty($r)){
             file_put_contents($pairs_filename, $r);
        $r = file_get_contents($pairs_filename);
    }
}
$r = explode("\n", $r);
$r = array_map('strtolower', $r);
$r = array_map('trim', $r);
foreach($r as $k=>$v){
    if(empty($v))                unset($r[$k]);
    if(substr($v, 0, 1) == '#')  unset($r[$k]);
    if(substr($v, 0, 2) == '//') unset($r[$k]);
}
$r = array_values($r);


$r = array_flip($r);
unset($r['busdusdt']);  // 2024-01-29 volume>0, empty kline websocket
unset($r['gbpusdt']);   // 2024-01-29 volume>0, empty kline websocket
$r = array_keys($r);


//$r = ['btcusdt','ethusdt'];


if(@$argv[1] == 'recreate'){
    $db->query("DROP DATABASE IF EXISTS `kline`");
    $db->query("CREATE DATABASE `kline`");
    $db->query("USE `kline`");
}


foreach($r as $v) kline_db_create_table($v);
foreach($r as $v) $client->kline($v, '1s', $callbacks);
