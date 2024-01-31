# kline

mysql: CREATE DATABASE `kline`

cd /home/kline

cat > .env.kline.php <<EOF
<?php
\$db_opts = array(
    'user'    => 'user',
    'pass'    => 'pass',
    'db'      => 'kline',
    'charset' => 'utf8mb4'
);
define('VERBOSE', true);
define('BINANCE_SPOT_KLINE_TBL_PREFIX', 'z_');  // binance_spot_kline_1s_btc_usdt  // binance_spot_kline_1s_btc_usdt_open_20240121
define('BINANCE_SPOT_KLINE_TBL_BY_DAYS', false);
EOF

git clone https://github.com/john252525/kline.git

mkdir composer

cd composer

composer require binance/binance-connector-php

composer require colshrapnel/safemysql

cd ..

nohup php -f kline_fill_via_websocket.php >> kline_log &

php -f kline_check_and_refill.php
