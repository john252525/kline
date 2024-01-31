<?php

require_once __DIR__ . '/../composer/vendor/autoload.php';


require_once __DIR__ . '/../.env.kline.php';  // $db_opts, BINANCE_SPOT_KLINE_TBL_PREFIX
require_once __DIR__.'/kline_functions.php';
$db = new SafeMySQL($db_opts);




$tables = $db->getCol("SHOW TABLES");
//$tables = ['btcusdt'];


$time = time();


foreach($tables as $v){
    if(VERBOSE) echo "\n".$v;
    
    if(!empty(BINANCE_SPOT_KLINE_TBL_PREFIX)  &&  substr($v, 0, strlen(BINANCE_SPOT_KLINE_TBL_PREFIX)) != BINANCE_SPOT_KLINE_TBL_PREFIX) continue;

    kline_check_and_refill($v);
}


if(VERBOSE) echo "\n" . 'exec_time: '. (time()-$time);
