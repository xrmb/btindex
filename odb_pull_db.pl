#!perl

use strict;

use Getopt::Long;
use DBI;
use Time::HiRes;

use btindex;


GetOptions('db=s' => \my $db);


my $tdb = new btindex::tdb(file => "dbs/$db");

my $config = btindex::config();
my $dbh = DBI->connect($config->{odb}, $config->{odb_user}, $config->{odb_pass}) || die;

my $offset = 0;
for(;;)
{
  my $q = qq|SELECT `hash` FROM `tdb` WHERE `db` = ? LIMIT 10000 OFFSET $offset|;
  my $sth = $dbh->prepare($q) || die;
  $sth->execute($db) || die;

  while(my $row = $sth->fetchrow_hashref())
  {
    $offset++;

    $tdb->sid($row->{hash}, add => \my $added);

    printf("%s\t%s\n", $row->{hash}, $added ? 'new' : 'old');
  }

  last unless($sth->rows());
}

$dbh->disconnect();
