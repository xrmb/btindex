#!/usr/bin/perl

use Getopt::Long;

use btindex;

use strict;


GetOptions(cleanup => \my $cleanup) || die;


my $dbf = new btindex::tdb(file => 'dbs/torrents_fs', save => 1_000_000);
my $dbg = new btindex::tdb(file => 'dbs/torrents_got');
$dbf->clear();


foreach_torrent(
  sub
  {
    my ($tf) = @_;

    my $tid = (split('/', $tf))[-1];
    my $idg = $dbg->sid($tid);

    printf("%s\t%s\n", $tid, $idg ? 'dupe' : '');

    if($cleanup && $idg)
    {
      unlink($tf);
      next;
    }

    my $idf = $dbf->sid($tid, add => 1);

    return;
  });
