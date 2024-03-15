<?php

require_once "db.php";

function handleRequest() {
  $r = $_REQUEST['r'] ?? '';
  switch( $r ) {
  case 'register':
    handleWorkerRegistration();
    break;
  default:
    http_response_code(400);
    return;
  }
}

handleRequest();

function genCookie() {
  $cookie = "";
  for( $i=0; $i<5; $i++ ) {
    $cookie .= sprintf("%x",rand(0,65535));
  }
  return substr($cookie,0,20);
}

function handleWorkerRegistration() {
  $hostname = $_REQUEST['hostname'];
  $ip4 = $_REQUEST['ip4'];
  $ip6 = $_REQUEST['ip6'];
  $server_port = $_REQUEST['server_port'];

  $dbh = connectDB();
  $sql = "
  INSERT INTO worker SET
    HOSTNAME = :HOSTNAME,
    IP4 = :IP4,
    IP6 = :IP6,
    SERVER_PORT = :SERVER_PORT,
    CREATED = NOW(),
    LAST_CONTACT = NOW(),
    COOKIE = :COOKIE
  ";
  $stmt = $dbh->prepare($sql);
  $stmt->bindValue(":HOSTNAME",$hostname);
  $stmt->bindValue(":IP4",$ip4);
  $stmt->bindValue(":IP6",$ip6);
  $stmt->bindValue(":SERVER_PORT",$server_port);
  $cookie = genCookie();
  $stmt->bindValue(":COOKIE",$cookie);
  $stmt->execute();

  $worker_id = $dbh->lastInsertId();

  $reply = array(
    'WORKER_ID' => $worker_id,
    'COOKIE' => $cookie,
  );
  echo json_encode($reply);
}
