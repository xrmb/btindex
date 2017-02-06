#!perl

use strict;

use DBI;

use btindex;


my $config = btindex::config();
my $dbh = DBI->connect($config->{odb}, $config->{odb_user}, $config->{odb_pass}) || die;

my $qd = qq|(SELECT `hash` FROM `tdb` WHERE LEFT(`hash`, 4) = ? AND `db` IN ('torrents_fs', 'torrents_got')) UNION (SELECT `hash` FROM `torrents` WHERE LEFT(`hash`, 4) = ?)|;
my $sthd = $dbh->prepare($qd) || die;

my $qi = qq|INSERT IGNORE INTO `torrents` (`hash`, `ts`, `data`) VALUES (?, FROM_UNIXTIME(?), ?)|;
my $sthi = $dbh->prepare($qi) || die;

my $total = 0;
my $l12;
my %d;
foreach_torrent(
  data => 'raw',
  sub
  {
    my ($tf, $d) = @_;

    my @stat = stat($tf);

    ### mediumblob size limit ###
    return if($stat[7] > 0xffffff);

    my @tf = split('/', $tf);

    if(substr($tf[-1], 0, 4) ne $l12)
    {
      $l12 = substr($tf[-1], 0, 4);
      $sthd->execute($l12, $l12) || die;
      %d = map { $_ => 1 } $sthd->fetchall_array([0]);
    }
    return if $d{$tf[-1]};

    $sthi->execute($tf[-1], $stat[9], $d) || die;

    ### skip if no insert ###
    return unless($sthi->rows());

    $total += $stat[7];
    printf("%s\t%s\t%d\n", $tf[-1], $stat[7], $total);

    return;
  });
