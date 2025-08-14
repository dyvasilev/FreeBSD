#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Cache::Memcached;
use POSIX qw(setsid);
use YAML::XS 'LoadFile';

#Config
my $path_yaml = "config/config.yaml";
my $cfg_yaml = LoadFile($path_yaml) or DIE ("Can't parse yaml cofig file");

my %cfg = (
	mysql_user => $cfg_yaml->{mysql}{user},
	mysql_pass => $cfg_yaml->{mysql}{pass},
	interval => 30,
	log_file => '/var/log/galera_status.log',
	memc_host => $cfg_yaml->{memcached}{host},
	memc_port => $cfg_yaml->{memcached}{port},
	memd => undef
);
my @nodes = ({name => 'node1',host => '127.0.0.1',port => 3307, dbh => undef},
	     {name => 'node2',host => '127.0.0.1',port => 3308, dbh => undef},
	     {name => 'node3',host => '127.0.0.1',port => 3310, dbh => undef}
);

sub log_msg {
	my($msg) = @_;
	print scalar(localtime)." - $msg\n";
}

sub init_daemon {
	chdir '/';
	open STDIN, '/dev/null';
	open STDOUT, '>>', $cfg{log_file} or die "Can't open log: $!";
	open STDERR, '>>', $cfg{log_file} or die "Can't open log: $!";
	defined(my $pid = fork) or die "Can't fork: $!";
	exit if $pid;
	setsid() or die "Can't start session: $!";
	log_msg("----------Daemon started----------");	
}
sub connect_mysql {
	my ($host, $port) = @_;
	return DBI->connect("DBI:MariaDB:host=$host;port=$port",
	  $cfg{mysql_user},
	  $cfg{mysql_pass},
	  { RaiseError => 0, PrintError => 0});
}

sub get_node_status {
	my ($dbh) = @_;
	if(!$dbh || !$dbh->ping) {
	  return "DOWN";
	}
	my $sth = $dbh->prepare("SHOW STATUS LIKE 'wsrep_local_state_comment'");
	return "DOWN" unless $sth && $sth->execute;
	my ($var, $value) = $sth->fetchrow_array;
	return defined $value ? $value : "UNKNOWN";
}
sub connect_memcached {
	$cfg{memd} = Cache::Memcached->new({
	  servers => [$cfg{memc_host}.':'.$cfg{memc_port}],
	  debug => 0,
	  compress_threshold => 10_000,
	});
}
sub monitor_loop {
	while(1) {
	 foreach my $node (@nodes) {
	  my $node_name = $node->{name};
	  my $host = $node->{host};
	  my $port = $node->{port};
	  $node->{dbh} //= connect_mysql($host, $port);
	  $cfg{memd} //= connect_memcached();
	  my $node_dbh = $node->{dbh};
	  my $memcached = $cfg{memd};

	  my $node_status = get_node_status($node_dbh);
	  if($node_status eq "Synced") {
	    $cfg{memd}->set($node_name, "$host:$port");
	    log_msg("Adding $node_name Synced");
          }
	  else {
	    $cfg{memd}->delete($node_name);
	    log_msg("Deleting $node_name");
	    next;
	  }
	  log_msg("$node_name"." = ".$cfg{memd}->get($node_name));	
    }
	 sleep $cfg{interval}; 	
  }
}
init_daemon();
monitor_loop();
