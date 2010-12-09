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

# tunable warning thresholds.
my $delay_warning_threshold = 5.0;
my $max_desync_txn_count_warning = 10;

# Connect to a database and return the database handle.
sub dbconnect (%) {
    my %params = @_;
    my $connstr = "dbi:Pg:" . 	('dbname=' . $params{'dbname'}) .
	(exists($params{'host'}) ? ';host=' . $params{'host'} : '') .
	(exists($params{'port'}) ? ';port=' . $params{'port'} : '') .
	(exists($params{'options'}) ? ';options=' . $params{'options'} : '');
	
    my $dbh = DBI->connect($connstr, $params{'username'}, $params{'password'});
    return $dbh;
}

# database handles for the master and slave.
my $dbmh = dbconnect(%master) || die "failed to connect to the master";
my $dbsh = dbconnect(%slave) || die "failed to connect to the slave";


# state information variables.
my ($last_sent, $last_recv, %timestamps);
my $desyncCount = 0;
my $idleMasterCount = 0;
my $idleSlaveCount = 0;
my $cleanupCount = 0;

while (1) {
    # Get the last sent transaction on the master.
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
    
    # Get the last replayed transaction on the slave.
    $res = $dbsh->selectall_arrayref("SELECT pg_last_xlog_replay_location()");
    $pos = $res->[0]->[0];
    if (!defined($last_recv)) {
	$last_recv = $pos;
    } elsif ($pos ne $last_recv) {
	$timestamps{$pos}{'recv'} = time();
	$last_recv = $pos;
	$idleSlaveCount = 0;

	if (exists($timestamps{$pos}{'sent'})) {
	    # found a transaction that we saw both the sent and recv times for.
	    my $delay = $timestamps{$pos}{'recv'} - $timestamps{$pos}{'sent'};
	    printf("Transaction %s took %.2f secs\n", $pos, $delay);
	    if ($delay > $delay_warning_threshold) {
		print "WARNING: Large replication delay detected... may indicate over utilization.\n";
	    }
	    $desyncCount = 0;

	    # clean out old transactions that are older than the match we just found.
	    if ($cleanupCount++ > 1000) {
		my $txn_sent = $timestamps{$pos}{'sent'};
		foreach my $txn (keys %timestamps) {
		    if ($timestamps{$txn}{'sent'} < $txn_sent) {
			delete $timestamps{$txn};
		    }
		}
		$cleanupCount = 0;
	    }
	} elsif ($desyncCount++ > $max_desync_txn_count_warning) {
	    print "WARNING: Waiting for matching transaction (try $desyncCount)... may indicate large desync.\n";
	}
    } else {
	$idleSlaveCount++;
    }

    # Check for idle states on the databases and print warnings.
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
	    print "ERROR: Database is not replicating but activity is detected on master.\n";
	}
	$idleSlaveCount %= 1000;
    }

    # Sleep to avoid spinning too fast.
    sleep(0.01);
};


