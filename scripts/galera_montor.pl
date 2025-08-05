#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use IO::Socket::INET;
use POSIX qw(setsid);
use Fcntl qw(:DEFAULT :flock);

#Config
my %cfg = (
	mysql_user => 'root',
	mysql_pass => '',
	interval => 30,
	log_file => '/var/log/galera_status.log',
	pid_file => '/var/run/galera_status.pid',
	memcached_host => '127.0.0.1',
	memcached_port => 11211
);
my @nodes = ({name => 'node1',socket => '/tmp/mysql-node1.sock'},
	     {name => 'node2',socket => '/tmp/mysql-node2.sock'},
	     {name => 'node3',socket => '/tmp/mysql-node3.sock'}
);
my %db_handles;
my %stmt_handles;
sub log_msg {
	my($msg) = @_;
	print scalar(localtime)." - $msg\n";
}

sub init_daemon {
	open(my $pidf, '>', $cfg{pid_file}) or die "Can't open pid file: $!";
	flock($pidf, LOCK_EX|LOCK_NB) or die "Already running. \n";
	print $pidf "$$\n";

	chdir '/';
	open STDIN, '/dev/null';
	open STDOUT, '>>', $cfg{log_file} or die "Can't open log: $!";
	open STDERR, '>>', $cfg{log_file} or die "Can't open log: $!";
	defined(my $pid = fork) or die "Can't fork: $!";
	exit if $pid;
	setsid() or die "Can't start session: $!";
	log_msg("Daemon started");	
}
sub connect_mysql {
	my ($socket) = @_;
	return DBI->connect("DBI:MariaDB:;mariadb_socket=$socket",
	 $cfg{mysql_user},
	 $cfg{mysql_pass},
	 { RaiseError => 0, PrintError => 0});
}
sub prepare_status_stmt {
my ($dbh) = @_;
return $dbh->prepare("SHOW STATUS LIKE 'wsrep_local_state_comment'");
}
sub get_galera_status {
	my ($sth) = @_;
	return "DOWN" unless $sth && $sth->execute;
	my ($var, $value) = $sth->fetchrow_array;
	return defined $value ? $value : "UNKNOWN";
}
sub connect_memcached {
	return IO::Socket::INET->new(
	PeerAddr => $cfg{memcached_host},
	PeerPort => $cfg{memcached_port},
	Proto => 'tcp',
	);
}
sub set_memcached_key {
	my ($sock, $key, $value) = @_;
	my $len = length($value);
	print  $sock "set $key 0 60 $len\r\n$value\r\n"
}

sub monitor_loop {
	while(1) {
	  my $memd = connect_memcached();
	  log_msg("Memcached unavailable.") unless $memd;
	  
	 foreach my $node (@nodes) {
	  my $name = $node->{name};
	  my $socket = $node->{socket};
	  my $dbh = $db_handles{name};
	  my $sth = $stmt_handles{name};

	 if(!$dbh||!$dbh->ping) {
	   $dbh = connect_mysql($socket);
	 if($dbh) {
	   $sth= prepare_status_stmt($dbh);
	   $db_handles{$name} = $dbh;
	   $stmt_handles{$name} = $sth;
	   log_msg("Reconnect to $name");			
	 } else {
	  log_msg("Failed to connect to $name");
	  next;
	 }
      }
	 my $status = get_galera_status($sth);
	 my $key = "galera_${name}_${status}";
	 if ($memd) {
	   set_memcached_key($memd, $key, $socket);
	   log_msg("$key => $socket");	
	 }
	}
	sleep $cfg{interval}; 	
  }
}
init_daemon();
monitor_loop();
