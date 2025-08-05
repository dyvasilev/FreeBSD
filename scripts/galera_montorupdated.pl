#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Cache::Memcached;
use IO::Socket::INET;
use POSIX qw(setsid);
use Fcntl qw(:DEFAULT :flock);

#Config
my %cfg = (
	mysql_user => 'test_user1',
	mysql_pass => 'test_pass',
	interval => 30,
	log_file => '/var/log/galera_status.log',
	pid_file => '/var/run/galera_status.pid',
	memc_host => '127.0.0.1',
	memc_port => 11211
);
my @nodes = ({name => 'node1',host => '127.0.0.1',port=>3307},
	     {name => 'node2',host => '127.0.0.1',port=>3308},
	     {name => 'node3',host => '127.0.0.1',port=>3310}
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
	my ($host, $port) = @_;
	return DBI->connect("DBI:MariaDB:host=$host;port=$port",
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
	return Cache::Memcached->new({
	servers=>[$cfg{memc_host}.':'.$cfg{memc_port}],
	debug =>0,
	compress_threshold=>10_000,
	});
}
sub monitor_loop {
	while(1) {
	  my $memd = connect_memcached();
	  log_msg("Memcached unavailable.") unless $memd;
	 foreach my $node (@nodes) {
	  my $name = $node->{name};
	  my $host = $node->{host};
	  my $port = $node->{port};
	  my $dbh = $db_handles{name};
	  my $sth = $stmt_handles{name};

	 if(!$dbh||!$dbh->ping) {
	   $dbh = connect_mysql($host,$port);
	 if($dbh) {
	   $sth= prepare_status_stmt($dbh);
	   $db_handles{$name} = $dbh;
	   $stmt_handles{$name} = $sth;			
	 } else {
	  log_msg("$name down");
	 # next;
	 }
        }
	 my $status = get_galera_status($sth);
	my $key = "galera_${name}_Synced";
	if($status eq "Synced"){
	 $memd->set($key, "$host:$port");
	 log_msg("Adding $key");
        }
	else {
	 $memd->delete($key);
	 log_msg("Deleting $key");
	next;
	}
	 log_msg("$key"." = ".$memd->get($key));	
    }
	sleep $cfg{interval}; 	
  }
}
init_daemon();
monitor_loop();
