<?php

if(!defined('VERBOSE'))                        define('VERBOSE', true);
if(!defined('BINANCE_SPOT_KLINE_TBL_PREFIX'))  define('BINANCE_SPOT_KLINE_TBL_PREFIX', 'z_');
if(!defined('BINANCE_SPOT_KLINE_TBL_BY_DAYS')) define('BINANCE_SPOT_KLINE_TBL_BY_DAYS', true);


function kline_select_from_db($tbl, $version = 0){

  //$tbl = BINANCE_SPOT_KLINE_TBL_PREFIX . $tbl;

    global $db;

    if(empty($version)){
        $q = $db->getAll("SELECT `id`,`o`,`h`,`l`,`c` FROM ?n WHERE 1 ORDER BY `id` DESC", $tbl);
        return $q;
    }

    if($version == 1){
        $q = $db->getRow("SELECT COUNT(`id`) AS `cnt`, MIN(`id`) AS `min`, MAX(`id`) AS `max` FROM ?n WHERE 1", $tbl);  // id = timestamp
        if(empty($q['cnt'])) return ['cnt'=>0,'min'=>0,'max'=>0];  // default values: cnt=0, min=NULL, max=NULL
        return $q;
    }

    if($version == 2){  // not used
        $q = $db->getCol("SELECT `id` FROM ?n WHERE 1", $tbl);  // id = timestamp
        if(!$q) return ['cnt'=>0,'min'=>0,'max'=>0];

        $r = [];
        $r['cnt'] = count($q);
        $r['min'] = min($q);
        $r['max'] = max($q);
        
        return $r;
    }
}


function kline_check($cnt, $min, $max){
    if(empty($cnt)) return true;

    if($max < (time()-10)) return false;
    
    $diff = ($max - $min + 1) - $cnt;
    if($diff > 50000) exit('ALARM kline_check(), check consistency');
    if(!empty($diff)) return false;

    return true;
}


function kline_check_and_refill($tbl, $r = ['cnt'=>0,'min'=>0,'max'=>0]){  // $r not used
    // 1) select from db
    // 2) data consistency check
    // 3) if(false) refill
    $kline_load_endTime = 0;
    $check = false;
    while(!$check){
        
        if(VERBOSE) echo "\t".'select';
        $r = kline_select_from_db($tbl, 1);
        
        $check = kline_check($r['cnt'], $r['min'], $r['max']);
        if(!$check){
            if(!VERBOSE) echo "\n".date('Y-m-d H:i:s').' REFILL '.$tbl.' '.$kline_load_endTime;

            for($n = 1; $n <= 3; $n++){
                if(VERBOSE) echo ' load';
                $res_load = kline_load_and_insert_into_db($tbl, '1s', 1000, 0, $kline_load_endTime);
                if(!empty($res_load[0][0])) $kline_load_endTime = $res_load[0][0]/1000-1;

                $sleep = $n*$n;
                if(VERBOSE) echo ' sleep'.$sleep;
                sleep($sleep);

                if(!empty($res_load[0][0])) break;
            }
            if(empty($res_load[0][0])) exit('ALARM kline_check_and_refill(), check kline_load()');
        }
        else {
            if(VERBOSE) echo ' checked';    
        }
    }

    if(!empty($kline_load_endTime)){
        global $db;
        $db->query("ALTER TABLE ?n ORDER BY `id` ASC", $tbl);
    }

    if(VERBOSE) echo ' OK';
}


function kline_load($pair = 'btcusdt', $interval = '1s', $limit = 1000, $startTime = 0, $endTime = 0){  // order by timestamp asc only  // binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
    // api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&limit=1000
    $p = [];
    $p['symbol'] = strtoupper($pair);  // caps required
    $p['interval'] = '1s';
    if(!empty($startTime)) $p['startTime'] = $startTime*1000;  // not required
    if(!empty($endTime))   $p['endTime']   = $endTime  *1000;  // not required
  //$p['timeZone']  = '';
    $p['limit'] = 1000;  // Default 500; max 1000
    $r = file_get_contents('https://api.binance.com/api/v3/klines?'.http_build_query($p));
    return json_decode($r, 1);
}


function kline_load_and_insert_into_db($pair = 'btcusdt', $interval = '1s', $limit = 1000, $startTime = 0, $endTime = 0){
    
    $len = strlen(BINANCE_SPOT_KLINE_TBL_PREFIX);
    if(!empty($len) && substr($pair, 0, $len) == BINANCE_SPOT_KLINE_TBL_PREFIX) $pair = substr($pair, $len);

    
    if(VERBOSE) echo ' refill';
    $r = kline_load($pair, $interval, $limit, $startTime, $endTime);
    if(VERBOSE) echo ' ' . count($r) . ' ' . $endTime;
    
    foreach($r as $v){
        $v = kline_convert_from_load_to_db($v);
        kline_insert_into_db($pair, $v);
    }

    return $r;
}


function kline_convert_from_load_to_db($a = []){
    $r = [];

    $r['delay'] = -1;

    $r['id'] = $a[0]/1000;
    $r['o']  = $a[1];
    $r['h']  = $a[2];
    $r['l']  = $a[3];
    $r['c']  = $a[4];
    
    $r['json'] = json_encode($a);  //// если в kline_insert_into_db() json не пишем в бд то можно закомментить чтобы не энкодить зря

    return $r;
}


function kline_convert_from_websocket_to_db($msg = ''){

    $a = !is_array($msg) ? json_decode($msg, 1) : $msg;

    /*
    $msg={"e":"kline","E":1706007953005,"s":"BTCUSDT","k":{"t":1706007952000,"T":1706007952999,"s":"BTCUSDT","i":"1s","f":3385553652,"L":3385553657,"o":"38920.53000000","c":"38920.52000000","h":"38920.53000000","l":"38920.52000000","v":"0.09540000","n":6,"x":true,"q":"3713.01827880","V":"0.06708000","Q":"2610.78915240","B":"0"}}

    Array
    (
        [e] => kline
        [E] => 1706007953005
        [s] => BTCUSDT
        [k] => Array
            (
                [t] => 1706007952000
                [T] => 1706007952999
                [s] => BTCUSDT
                [i] => 1s
                [f] => 3385553652
                [L] => 3385553657
                [o] => 38920.53000000
                [c] => 38920.52000000
                [h] => 38920.53000000
                [l] => 38920.52000000
                [v] => 0.09540000
                [n] => 6
                [x] => 1
                [q] => 3713.01827880
                [V] => 0.06708000
                [Q] => 2610.78915240
                [B] => 0
            )
    
    )
    */

    $r = [];

    $ts = $a['k']['t']/1000;

    $r['delay'] = time() - $ts;

    $r['id']   = $ts;
    $r['o']    = $a['k']['o'];
    $r['h']    = $a['k']['h'];
    $r['l']    = $a['k']['l'];
    $r['c']    = $a['k']['c'];
    if(!is_array($msg)) $r['json'] = $msg;

    
    $r['pair'] = strtolower($a['k']['s']);


    return $r;
}


function kline_insert_into_db($tbl, $a = []){

    $tbl = BINANCE_SPOT_KLINE_TBL_PREFIX . $tbl;

    $allowed = [  // список должен быть не больше чем задан в kline_db_create_table()
                   'id',
                   'delay',
                   'o',
                   'h',
                   'l',
                   'c',
                 //'json',
               ];

    if(empty($a['json']) && in_array('json', $allowed)){
        $a['json'] = json_encode($a);
    }
    
    global $db;
    $ins = $db->filterArray($a, $allowed);
    $db->query("INSERT IGNORE INTO ?n SET ?u", $tbl, $ins);




    if(BINANCE_SPOT_KLINE_TBL_BY_DAYS && !empty($a['id'])){
        $ts = $a['id'];
        
        $cur_tbl = $tbl.'_'.(int)floor($ts/86400);
        try {
            $db->query("INSERT IGNORE INTO ?n SET ?u", $cur_tbl, $ins);
        }
        catch (Exception $e) {
            $len = strlen(BINANCE_SPOT_KLINE_TBL_PREFIX);
            $tmp = (!empty($len) && substr($cur_tbl, 0, $len) == BINANCE_SPOT_KLINE_TBL_PREFIX) ? substr($cur_tbl, $len) : $cur_tbl;
            kline_db_create_table($tmp);
            $db->query("INSERT IGNORE INTO ?n SET ?u", $cur_tbl, $ins);
        }
    }

}


function kline_slide($kline = ['open'=>[],'high'=>[],'low'=>[],'close'=>[]], $timeframe = [60,300,900,3600,3600*4,86400]){  // 180,1800
    /*
    $chunk = [];
    $tf = [];  // TimeFrame
    foreach($timeframe as $v){
        $chunk[$v]['low']  = array_chunk($kline['low'],  $v);
        $chunk[$v]['high'] = array_chunk($kline['high'], $v);

        $tf[$v]['low']  = array_map('min', $chunk[$v]['low']);
        $tf[$v]['high'] = array_map('max', $chunk[$v]['high']);

        $tf[$v]['low']  = array_reverse($tf[$v]['low']);
        $tf[$v]['high'] = array_reverse($tf[$v]['high']);
    }
    */

    $tf = [];  // TimeFrame
    //$kline['low'] = $kline['high'] = $kline['close'] = $kline['open'] = [10,23,234,  54,23,243543,  1,4546,3,  2345,46564,325434,  6,2343,34534,  353534,1,23,  24,100];    foreach([3] as $v){  // test data
    foreach($timeframe as $v){
        $tf[$v]['low']   =  array_reverse(array_map('min',         array_chunk($kline['low'],   $v)));
        $tf[$v]['high']  =  array_reverse(array_map('max',         array_chunk($kline['high'],  $v)));
        $tf[$v]['close'] = @array_reverse(array_map('array_shift', array_chunk($kline['close'], $v)));  // @ т.к. array_shift/pop пытаются уменьшить массив и кидают warning
        $tf[$v]['open']  = @array_reverse(array_map('array_pop',   array_chunk($kline['open'],  $v)));  // для close берем первый элемент (shift), а для open берем последний элемент (pop), потому что изначально DESC
    }
    return $tf;
}


function kline_slide_transpose($a = []){
    // двумерный числовой массив можно транспонировать еще проще:
    // array_unshift($array, null); $array = call_user_func_array("array_map", $array);  // php.net/manual/en/function.array-map.php#86743

    $keys = array_keys($a);  // ['low','high','close','open']
    if(empty($keys)) return [];
    if(!is_array($a[$keys[0]])) exit('ERROR: not two-dimensional array');
    $cnt = count($a[$keys[0]]);
    $r = [];
    for($i=0; $i<$cnt; $i++){
        foreach($keys as $v){
            $r[$i][$v] = $a[$v][$i];  // если !isset($a[$v][$i]) значит некорректная структура массива $a
        }
    }
    return $r;
}


function kline_slide_transpose_recursive($a = []){
    $r = [];
    foreach($a as $k=>$v){  // $k целое число (таймфрейм в секундах);  $v=['open'=>[],'high'=>[],'low'=>[],'close'=>[]]
        $r[$k] = kline_slide_transpose($v);
    }
    return $r;
}


function kline_slide_convert_for_ctrader($a = [], $ts = 0){
    $r = ['k'=>[
          //'my_dt'=>date('Y-m-d H:i:s', $ts),
            't'=>$ts*1000,
            'T'=>$ts*1000+1000-1,
            'o'=>$a['open'],
            'h'=>$a['high'],
            'l'=>$a['low'],
            'c'=>$a['close'],
        ]
    ];
    return $r;
}


function kline_slide_convert_for_ctrader_recursive($a = [], $ts = 0, $timeframe = 0){  // $timeframe в секундах
    $r = [];
    foreach($a as $v){
        $v = kline_slide_convert_for_ctrader($v, $ts);
        $r[] = json_encode($v);
        $ts += $timeframe;
    }
    $r = implode("\n", $r);
    return $r;
}


function kline_db_create_table($tbl = ''){

  // при изменении структуры таблицы проверить список полей в kline_insert_into_db() $allowed

    $tbl = BINANCE_SPOT_KLINE_TBL_PREFIX . $tbl;

    global $db;
    $db->query("CREATE TABLE IF NOT EXISTS ?n (
        `id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
        `delay` int(11) NOT NULL DEFAULT 0,
        `o` varchar(250) NOT NULL DEFAULT '',  # `o` double NOT NULL DEFAULT 0,
        `h` varchar(250) NOT NULL DEFAULT '',  # `h` double NOT NULL,
        `l` varchar(250) NOT NULL DEFAULT '',  # `l` decimal(18,8) NOT NULL DEFAULT 0,
        `c` varchar(250) NOT NULL DEFAULT '',  # `c` decimal(18,8) NOT NULL,
      # `json` text NOT NULL DEFAULT '',
        PRIMARY KEY (`id`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci", $tbl);


  //$db->query("TRUNCATE TABLE ?n", $tbl);
}
