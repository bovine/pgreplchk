#!/usr/bin/perl

# pgreplchk.pl -- PostgreSQL 9.x replication checker
# https://github.com/bovine/pgreplchk
#
# Connects simultaneously to two PostgreSQL databases and checks the
# Streaming Replication status, by comparing the transaction ids and
# computing the replication delay.
#


use strict;
use DBI;
require DBD::Pg;
use Time::HiRes qw(time sleep);


my %master = ( 
    dbname => 'mydb',
    host => 'remotehost.example.com',
    username => 'pgsql'
    );
my %slave = (
    dbname => 'mydb',
    host => 'localhost',
    username => 'pgsql'
    );

sub dbconnect (%) {
    my %params = @_;
    my $connstr = "dbi:Pg:" . 	('dbname=' . $params{'dbname'}) .
	(exists($params{'host'}) ? ';host=' . $params{'host'} : '') .
	(exists($params{'port'}) ? ';port=' . $params{'port'} : '') .
	(exists($params{'options'}) ? ';options=' . $params{'options'} : '');
	
    my $dbh = DBI->connect($connstr, $params{'username'}, $params{'password'});
    return $dbh;
}

my $dbmh = dbconnect(%master) || die;
my $dbsh = dbconnect(%slave) || die;


my ($last_sent, $last_recv, %timestamps);
my $desyncCount = 0;
my $idleMasterCount = 0;
my $idleSlaveCount = 0;

while (1) {
    my $res = $dbmh->selectall_arrayref("SELECT pg_current_xlog_location()");
    my $pos = $res->[0]->[0];
    if (!defined($last_sent)) {
	$last_sent = $pos;
    } elsif ($pos ne $last_sent) {
	$timestamps{$pos}{'sent'} = time();
	$last_sent = $pos;
	$idleMasterCount = 0;
    } else {
	$idleMasterCount++;
    }
    
    $res = $dbsh->selectall_arrayref("SELECT pg_last_xlog_replay_location()");
    $pos = $res->[0]->[0];
    if (!defined($last_recv)) {
	$last_recv = $pos;
    } elsif ($pos ne $last_recv) {
	$timestamps{$pos}{'recv'} = time();
	$last_recv = $pos;
	$idleSlaveCount = 0;

	if (exists($timestamps{$pos}{'sent'})) {
	    my $delay = $timestamps{$pos}{'recv'} - $timestamps{$pos}{'sent'};
	    printf("Transaction %s took %.2f secs\n", $pos, $delay);
	    $desyncCount = 0;
	} elsif ($desyncCount++ > 10) {
	    print "WARNING: Waiting for matching transaction (try $desyncCount)... may indicate large desync.\n";
	}
    } else {
	$idleSlaveCount++;
    }

    if ($idleMasterCount > 1000 && $idleSlaveCount > 1000) {
	if ($last_sent eq $last_recv) {
	    print "Databases are idle and identical.\n";
	} else {
	    print "ERROR: Database is not replicating and no database activity detected.\n";
	}
	$idleMasterCount %= 1000;
	$idleSlaveCount %= 1000;
    } elsif ($idleSlaveCount > 1000) {
	if ($last_sent ne $last_recv) {
	    print "ERROR: Database is not replication replication but activity is detected on master.\n";
	}
	$idleSlaveCount %= 1000;
    }

    sleep(0.01);
};


