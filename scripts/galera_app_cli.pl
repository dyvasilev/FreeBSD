#!/usr/bin/perl
use warnings;
use strict;
use YAML::XS 'LoadFile';
use Cache::Memcached;
use DBI;
my $cnf_yaml_path = "config/config.yaml";
my $cnf_yaml = LoadFile($cnf_yaml_path) or DIE ("YAML cofig file not parsed");

my $cfg_mysql = $cnf_yaml->{mysql};
my $cfg_nodes = $cnf_yaml->{nodes_cofig};
my $cfg_memc = $cnf_yaml->{memcached};

my %cfg = (
	mysql_user => $cfg_mysql->{user},
	mysql_pass => $cfg_mysql->{pass},
	memc_host => $cfg_memc->{host},
	memc_port => $cfg_memc->{port},
	database => $cfg_memc->{dtatabase},
	table => $cfg_memc->{table},
	log_file => $cnf_yaml->{app_cli}{log_file},
	num_nodes => 4	
);
my $dbi = undef;
sub connect_node {
     	my ($host,$port) = @_;
 	 $dbi= DBI->connect("DBI:MariaDB:host=$host;port=$port",
	 $cfg_mysql->{user},
	 $cfg_mysql->{pass},
	 {RaiseError=>1, PrintError=>1}
    	);
}
sub disconnect_node {
	$dbi->disconnect if $dbi;
}	
sub create_prepare_db {
     if($dbi) {
	my $database = $cfg_mysql->{database};
	my $table = $cfg_mysql->{table};
	$dbi->do("create database if not exists $database");
	$dbi->do("use $database");
	$dbi->do("create table if not exists 
		$table (recno INT AUTO_INCREMENT PRIMARY KEY, 
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
                my $sth = $dbi->prepare("insert into $cfg_mysql->{table}(user_recno,money_amount,ts_unix_timestamp) values (?,?,UNIX_TIMESTAMP())");
		$sth->execute($user_recno, $ammount);
	}
     }
else {print("Can't insert records\n");}
     
}
sub return_sum_amount {
      if($dbi){
	my $res =$dbi->prepare("select recno, sum(money_amount) as total, count(recno) as record_count from $cfg_mysql->{table} group by user_recno");
        $res->execute;
	while (my $row=$res->fetchrow_arrayref){
	      print join("\t",@$row),"\n";
        }
      }
	else {print("Cant return summ")}
}
sub flush_records {
	if($dbi){
	     my $del = $dbi->prepare("truncate table $cfg_mysql->{table}");
	     $del->execute;
	}
}
my $memd = Cache::Memcached->new({servers => [ $cfg_memc->{host}.':'.$cfg_memc->{port}]});
sub pick_first_work_node {
	for (my $i =1;$i < $cfg{num_nodes}; $i++) {
	   my $snode = $memd->get("node${i}");
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
