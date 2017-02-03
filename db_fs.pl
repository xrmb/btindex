#!/usr/bin/perl

use Getopt::Long;

use btindex;

use strict;


GetOptions(cleanup => \my $cleanup) || die;


my $dbfo = new btindex::tdb(file => 'dbs/torrents_fs', save => -1);
my $dbfn = new btindex::tdb(file => 'dbs/torrents_fs', save => 1_000_000);
my $dbg = new btindex::tdb(file => 'dbs/torrents_got');
$dbfn->clear();

my $files = 0;
my $size = 0;
foreach_torrent(
  sub
  {
    my ($tf) = @_;

    my $tid = (split('/', $tf))[-1];
    my $idg = $dbg->sid($tid);

    $files++;
    $size += -s $tf;

    printf("%s\t", $tid);

    if($idg)
    {
      print("dupe\n");
      if($cleanup && $idg)
      {
        unlink($tf);
      }
      next;
    }

    my $idfo = $dbfo->sid($tid);
    my $idfn = $dbfn->sid($tid, add => \my $added);

    printf("%s\t%d files\t%1.f mb\n", $idfo ? 'old' : 'new', $files, $size/1024/1024);
    return;
  });
