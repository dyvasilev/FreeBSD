#!/usr/bin/perl
use warnings;
use strict;


my ($action, $node_id) = @ARGV;

my $usage = "start|stop nodeid initcluster";
if(@ARGV < 2 || $action ne "start" && $action ne "stop"||!$node_id) {
  die($usage);
}
my @ACTIVE_SOCKETS = glob("/tmp/mysql-node*.sock");
my $SOCKET_PATH = "/tmp/mysql-node${node_id}.sock";
my $CONF_PATH = "/usr/local/etc/mysql/my-node${node_id}.cnf";
my $CMD_START = "sudo -u mysql /usr/local/libexec/mysqld --defaults-file=${CONF_PATH}";
my $CMD_LOG = "tail -f /var/log/mysql/mysql-node${node_id}.err";
if(!@ACTIVE_SOCKETS) {
	$CMD_START .= " --wsrep-new-cluster";
}
$CMD_START .= "&";
my $CMD_STOP = "mysqladmin --socket=${SOCKET_PATH} shutdown&";
my $NODE_STARTED = grep $_ eq $SOCKET_PATH, @ACTIVE_SOCKETS; 
my $cmd;
if($action eq "start" && $node_id){
	if(!$NODE_STARTED) {
           $cmd = $CMD_START;	
	}
	else {die "$SOCKET_PATH already exis";}
}
elsif($action eq "stop" && $node_id){
	if($NODE_STARTED) {
      	   $cmd = $CMD_STOP;
	}
	else {die "$SOCKET_PATH not found.";}
}
system($cmd) == 0 or die "Failed to execute";
system($CMD_LOG);

