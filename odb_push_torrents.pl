#!perl

use strict;

use DBI;

use btindex;


my $config = btindex::config();
my $dbh = DBI->connect($config->{odb}, $config->{odb_user}, $config->{odb_pass}) || die;

my $q = qq|INSERT IGNORE INTO `torrents` (`hash`, `ts`, `data`) VALUES (?, FROM_UNIXTIME(?), ?)|;
my $sth = $dbh->prepare($q) || die;

foreach_torrent(
  data => 1,
  sub
  {
    my ($tf, $d) = @_;

    my @tf = split('/', $tf);
    printf("%s\n", $tf[-1]);

    my @stat = stat($tf);

    return if($stat[7] > 0xffffff);

    $sth->execute($tf[-1], $stat[9], $d) || die;

    return;
  });