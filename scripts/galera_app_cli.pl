#!/usr/bin/perl
use warnings;
use strict;

use Cache::Memcached;
use DBI;
my %cfg = (
	mysql_user => 'test_user1',
	mysql_pass => 'test_pass',
	memc_host => '127.0.0.1',
	memc_port => 11211,
	database => 'testDb',
	table => 'testTBL',
	log_file => '/var/log/galera_app.log',
	num_nodes => 4	
);
my $dbi = undef;
sub connect_node {
     	my ($host,$port) = @_;
 	 $dbi= DBI->connect("DBI:MariaDB:host=$host;port=$port",
	 $cfg{mysql_user},
	 $cfg{mysql_pass},
	 {RaiseError=>1, PrintError=>1}
    	);
}
sub disconnect_node {
	$dbi->disconnect if $dbi;
}	
sub create_prepare_db {
     if($dbi) {
	$dbi->do("create database if not exists $cfg{database}");
	$dbi->do("use $cfg{database}");
	$dbi->do("create table if not exists 
		$cfg{table} (recno INT AUTO_INCREMENT PRIMARY KEY, 
		user_recno INT UNSIGNED, 
		money_amount decimal(10,2), 
		ts_unix_timestamp int unsigned)"); 	
      }
 else {print("Can't create table\n");}
}
sub insert_records {
     if($dbi){
	for (my $i = 0; $i < 1000; $i++) {
		my $user_recno = int(rand(50));
		my $ammount = sprintf("%.2f", rand(5000));
  my $sth = $dbi->prepare("insert into $cfg{table}(user_recno,money_amount,ts_unix_timestamp) values (?,?,UNIX_TIMESTAMP())");
		$sth->execute($user_recno, $ammount);
	}
     }
else {print("Can't insert records\n");}
     
}
sub return_sum_amount {
      if($dbi){
	my $res =$dbi->prepare("select recno, sum(money_amount) as total, count(recno) as record_count from $cfg{table} group by user_recno");
        $res->execute;
	while (my $row=$res->fetchrow_arrayref){
	 	print join("\t",@$row),"\n";
        }
      }
	else {print("Cant return summ")}
}
sub flush_records {
	if($dbi){
	 my $del = $dbi->prepare("truncate table $cfg{table}");
	$del->execute;
	}
}
my $memd = Cache::Memcached->new({servers => [ $cfg{memc_host}.':'.$cfg{memc_port}]});
sub pick_first_work_node {
	for (my $i =1;$i < $cfg{num_nodes}; $i++) {
	   my $snode = $memd->get("galera_node${i}_Synced");
           if(defined $snode){
 		return (split /:/, $snode);
	    }
        }
 }

my ($host, $port) = pick_first_work_node();
if($host&&$port){
	connect_node($host,$port);
	create_prepare_db();
	insert_records();
	return_sum_amount();
	flush_records();
	disconnect_node();
	print ("ActiveNode->$host:$port\n");
}
else{
	print "No active node is found!\n"
}
