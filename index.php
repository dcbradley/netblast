<?php
ini_set('display_errors', 'On');

require_once "db.php";

function handleRequest() {
  $r = $_REQUEST['r'] ?? '';
  switch( $r ) {
  case 'register':
    handleWorkerRegistration();
    break;
  case 'getwork':
    handleGetWork();
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

function isIP4($addr) {
  if( preg_match('^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$',$addr) ) {
    return true;
  }
  return false;
}

function isIP6($addr) {
  if( preg_match('^[0-9a-f:]+$',$addr) ) {
    return true;
  }
  return false;
}

function handleWorkerRegistration() {
  $hostname = $_REQUEST['hostname'];
  $ip4 = $_REQUEST['ip4'] ?? null;
  $ip6 = $_REQUEST['ip6'] ?? null;
  if( $ip4 === null && isIP4($_SERVER['REMOTE_ADDR']) ) {
    $ip4 = $_SERVER['REMOTE_ADDR'];
  }
  if( $ip6 === null && isIP6($_SERVER['REMOTE_ADDR']) ) {
    $ip6 = $_SERVER['REMOTE_ADDR'];
  }
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

function handleGetWork() {
  $worker_id = $_REQUEST['worker_id'];
  $cookie = $_REQUEST['cookie'];
  $requested_mode = $_REQUEST['mode'] ?? null;

  $dbh = connectDB();
  $sql = "SELECT * FROM worker WHERE WORKER_ID = :WORKER_ID AND COOKIE = :COOKIE";

  $stmt = $dbh->prepare($sql);
  $stmt->bindValue(":WORKER_ID",$worker_id);
  $stmt->bindValue(":COOKIE",$cookie);
  $stmt->execute();
  $row = $stmt->fetch();

  if( !$row ) {
    $reply = array('SUCCESS' => false, 'ERROR_MSG' => 'Failed to find worker with specified ID.');
    echo json_encode($reply);
    return;
  }

  # Unless a mode was requested, be a client if there is an available
  # server.  Otherwise, be a server.

  if( !$requested_mode || $requested_mode == 'client' ) {
    $sql = "
      SELECT
        worker.WORKER_ID,
        worker.SERVER_PORT,
        worker.IP4,
        worker.IP6
      FROM
        worker
      LEFT JOIN
        connection
      ON
        connection.SERVER_ID = worker.WORKER_ID
      WHERE
        worker.CLOSED IS NULL
        AND worker.SERVER_PORT IS NOT NULL
        AND connection.SERVER_ID IS NULL
        AND timestampdiff(SECOND,NOW(),worker.LAST_CONTACT) < 600
      LIMIT 1
    ";
    $stmt = $dbh->prepare($sql);
    $stmt->execute();
    $row = $stmt->fetch();
    if( $row ) {
      $server_ip = $row['IP4'] ? $row['IP4'] : $row['IP6'];
      $args = array('-p',$row['SERVER_PORT'],'-c',$server_ip);
      $reply = array(
        'SUCCESS' => true,
        'CMD' => 'iperf',
        'MODE' => 'client',
        'ARGS' => $args,
      );
      echo json_encode($reply);
      return;
    }
  }

  if( !$requested_mode || $requested_mode == 'server' ) {
    $args = array('-s');
    $reply = array(
      'SUCCESS' => true,
      'CMD' => 'iperf',
      'MODE' => 'server',
      'ARGS' => $args,
    );
    echo json_encode($reply);
  }
}
