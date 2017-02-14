#!perl

use strict;

use DBI;

use btindex;


my $config = btindex::config();
my $dbh = DBI->connect($config->{odb}, $config->{odb_user}, $config->{odb_pass}) || die;

my $qd = qq|(SELECT `hash` FROM `tdb` WHERE `l1` = ? AND `l2` = ? AND `db` IN ('torrents_fs', 'torrents_got')) UNION (SELECT `hash` FROM `torrents` WHERE `l1` = ? AND `l2` = ?)|;
my $sthd = $dbh->prepare($qd) || die;

my $qi = qq|INSERT IGNORE INTO `torrents` (`hash`, `l1`, `l2`, `ts`, `data`) VALUES (?, ?, ?, FROM_UNIXTIME(?), ?)|;
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

    if(0 && substr($tf[-1], 0, 4) ne $l12)
    {
      my $l1 = substr($tf[-1], 0, 2);
      my $l2 = substr($tf[-1], 2, 2);
      $l12 = $l1.$l2;
      $sthd->execute($l1, $l2, $l1, $l2) || die;
      %d = map { $_ => 1 } @{$sthd->fetchall_arrayref([0])};
    }
    return if $d{$tf[-1]};

    $sthi->execute($tf[-1], substr($tf[-1], 0, 2), substr($tf[-1], 2, 2), $stat[9], $d) || die;

    ### skip if no insert ###
    return unless($sthi->rows());

    $total += $stat[7];
    printf("%s\t%s\t%d\n", $tf[-1], $stat[7], $total);

    return;
  });
