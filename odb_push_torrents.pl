#!perl

use strict;

use DBI;

use btindex;


my $config = btindex::config();
my $dbh = DBI->connect($config->{odb}, $config->{odb_user}, $config->{odb_pass}, { mysql_auto_reconnect => 1 }) || die;

my $qd = qq|(SELECT `hash`, `db` FROM `tdb` WHERE `l1` = ? AND `l2` = ? AND `db` IN ('torrents_fs', 'torrents_got')) UNION (SELECT `hash`, 'torrents' AS `db` FROM `torrents` WHERE `l1` = ? AND `l2` = ?)|;
my $sthd = $dbh->prepare($qd) || die;

my $qi = qq|INSERT IGNORE INTO `torrents` (`hash`, `l1`, `l2`, `ts`, `data`) VALUES (?, ?, ?, FROM_UNIXTIME(?), ?)|;
my $sthi = $dbh->prepare($qi) || die;

my $total = 0;
my $l12;
my %d;
foreach_torrent(
  data => 'raw',
  start => $ARGV[0],
  sub
  {
    my ($tf, $d) = @_;

    my @stat = stat($tf);

    ### mediumblob size limit ###
    return if($stat[7] > 0xffffff);

    my @tf = split('/', $tf);

    for(;;)
    {
      if(substr($tf[-1], 0, 4) ne $l12)
      {
        my $l1 = substr($tf[-1], 0, 2);
        my $l2 = substr($tf[-1], 2, 2);
        $l12 = $l1.$l2;
        print("$l12...\n");
        $sthd->execute($l1, $l2, $l1, $l2) || warn($sthi->errstr) && sleep(10) && next;
        %d = map { $_->{hash} => $_->{db} } @{$sthd->fetchall_arrayref({})};
      }
      if($d{$tf[-1]})
      {
        printf("%s\t%s\n", $tf[-1], $d{$tf[-1]});
        return;
      }

      $sthi->execute($tf[-1], substr($tf[-1], 0, 2), substr($tf[-1], 2, 2), $stat[9], $d) || warn($sthi->errstr) && sleep(10) && next;

      ### skip if no insert ###
      return unless($sthi->rows());

      $total += $stat[7];
      printf("%s\t%-20s\t%s\t%d\n", $tf[-1], 'new', $stat[7], $total);

      return;
    }
  });
