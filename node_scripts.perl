#!/usr/bin/perl
use warnings;
use strict;
use YAML::XS "LoadFile";

my ($action, $node_id) = @ARGV;

my $usage = "start|stop nodeid initcluster";
if(@ARGV < 2 || $action ne "start" && $action ne "stop"||!$node_id) {
  die($usage);
}
my $cnf_yaml_path = "config/config.yaml";
my $cnf_yaml = LoadFile($cnf_yaml_path) or DIE ("YAML config file not found");
my $mysql_conf = $cnf_yaml->{mysql};
my $node_conf = $cnf_yaml->{nodes_config};

my $sockets_folder = "$node_conf->{sockets_path}"; 
my $node_socket = "$sockets_folder/mysql-node${node_id}.sock"; 
my $node_cfg_file = "$node_conf->{node_cfg_path}/my-node${node_id}.cnf"; 
my $log_file = "$node_conf->{log_path}/mysql-node${node_id}.err"; 
my @ACTIVE_SOCKETS = glob("$sockets_folder/mysql-node*.sock");


my $CMD_START = "sudo -u mysql $mysql_conf->{mysqld} --defaults-file=${node_cfg_file}";
my $CMD_LOG = "tail -f $log_file";
if(!@ACTIVE_SOCKETS) {
	$CMD_START .= " --wsrep-new-cluster";
}
$CMD_START .= "&";
my $CMD_STOP = "mysqladmin --socket=${node_socket} shutdown&";
my $NODE_STARTED = grep $_ eq $node_socket, @ACTIVE_SOCKETS; 
my $cmd;
if($action eq "start" && $node_id){
	if(!$NODE_STARTED) {
           $cmd = $CMD_START;	
	}
	else {die "$node_socket already exis";}
}
elsif($action eq "stop" && $node_id){
	if($NODE_STARTED) {
      	   $cmd = $CMD_STOP;
	}
	else {die "$node_socket not found.";}
}
system($cmd) == 0 or die "Failed to execute";
system($CMD_LOG);

