#!/usr/bin/perl

use threads;
use threads::shared;
use Thread::Queue;

#use LWP;
use HTTP::Tiny;
use Getopt::Long;
use Term::ReadKey;
use Win32::DriveInfo;
use Win32::Console;
use Convert::Bencode;
#use Convert::Bencode_XS;


use Whatsup;
use btindex;

use strict;


my $count :shared = 0;
sub p
{
  my ($t, $tid) = @_;

  my @h = (
      #"https://itorrents.org/torrent/%s.torrent",
      "https://www.skytorrents.in/file/%s/hh"
    );
  #while(int(rand() * 3) == 0) { push(@h, shift(@h)); }

  #my $ua = LWP::UserAgent->new;
  #$ua->timeout(30);
  #$ua->agent('Mozilla/5.0 (Windows NT 6.2; WOW64; rv:32.0) Gecko/20100101 Firefox/52.0');
  #$ua->requests_redirectable([]);

  my $ua;

  my $tf = btindex::torrent_path($tid);


  my @url = map { sprintf($_, $tid) } @h;

  #printf("%s\t", $tid) unless($quiet);
  #push(@h, shift(@h));
  my $anyok = 0;
  my $got;
  my $url;
  while(@url)
  {
    if(!$ua) { $ua = new HTTP::Tiny(max_redirect => 0, agent => 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:32.0) Gecko/20100101 Firefox/52.0', timeout => 10); }

    $url = shift(@url);

    my $response = $ua->get($url);#, Referer => $url);

    my $dc = $response->{content};#decoded_content();
    my $code = $response->{status};#code();
    my $ok = $response->{success};#is_success();

    if($code == 500)
    {
      $dc =~ s/\n.*//g;
      print("$t\t$tid\t500\t$dc\n");# unless($quiet);
      #unshift(@url, $url);
      #sleep(1);
      undef $ua;
      redo;
    }

    $anyok = 1;
    if($ok && $dc eq 'torrent file is not here.' || $dc =~ /404 - File not found/ || $dc eq '')
    {
      $ok = 0;
      $code = 404;
      $dc = '';
    }

    if($ok && ($dc !~ m/^d/ || $dc !~ /e$/))
    {
      print("$t\t$tid\tnot a torrent\t$url\n");# unless($quiet);
      #write_file('s:/not_a_torrent.dat', $dc);
      next;
    }

    if($ok)
    {
      eval { Convert::Bencode_XS::bdecode($dc); };
      #eval { Convert::Bencode::bdecode($dc); };
      if($@)
      {
        print("$t\t$tid\tdecode error\n");# unless($quiet);

        $dc =~ s/:([\w\.]+)i-?\d.\d+E\+\d+e/:${1}i1e/;
        eval { Convert::Bencode_XS::bdecode($dc); };
        #eval { Convert::Bencode::bdecode($dc); };
        if($@)
        {
          print("$t\t$tid\tdecode error final $@\n");# unless($quiet);
          next;
        }

        print("$t\t$tid\tfixed!\n");# unless($quiet);
      }
    }

    if($ok)
    {
      my $ihc = btindex::torrent_infohash3($dc);
      if($ihc ne $tid)
      {
        print("$t\t$tid\tinfohash mismatch\n");# unless($quiet);
        next;
      }
    }

    if($ok && length($dc))
    {
      #print(length($dc)) unless($quiet);
      $url =~ s!^https?://|/.*$!!g;
      #print(("\t" x (scalar(@url)+1)).$url) unless($quiet);
      write_file($tf, $dc);
      $count++;
      $got = length($dc)."\t".$url;
      last;
    }

    #print($code."\t");
    if(@url && ($code == 404 || $ok && length($dc) == 0))
    {
      next;
    }
  }

  if(!-f $tf && $anyok)
  {
    print("$t\t$tid\t404\n");# unless($quiet);
    return ($tid, 'n404');
  }

  print("$t\t$tid\t$got\n");# unless($quiet);
  return ($tid, 'ok', $url);
}




my $quiet = 0;
my $fid = undef;
my $start;
my $db = 'rss';
my $threads = 1;

GetOptions(quiet => \$quiet, 'filter=s' => \$fid, 'db=s' => \$db, 'start=s' => \$start, 'threads=i' => \$threads) || die;

$threads--;

#run_only_once() unless(defined($fid));
$| = 1;# unless(defined($fid));



my $dbgot = new btindex::tdb(file => 'dbs/torrents_got');

my $con = new Win32::Console();


my $q;
my $stat = {};

my $exit = 0;
my @threads;
if($threads)
{
  $q = Thread::Queue->new();
  $q->limit = $threads*2;
  share($exit);
  share($stat);

  $stat = shared_clone({});

  ### worker threads ###
  @threads = map {
    threads->create(sub
    {
      my ($t) = @_;
      #print("thread $t started\n");

      while(defined(my $tid = $q->dequeue()))
      {
        last if($exit);

        conup(p($t, $tid));
        #p($tid);
      }
      #print("thread $t done\n");
      return;
    }, $_);
  } 0..$threads;
}


sub conup
{
  my ($tid, $r, $url) = @_;

  $stat->{$r}++;
  #if($r eq 'ok')
  #{
  #  my $domain = (split(/\//, $url))[2];
  #  $stat->{d}{$domain}++;
  #}
  $stat->{att}++;

  $stat->{title} = sprintf("%d of %d|%.2f%%|%d|%s", $stat->{ok}, $stat->{att}, 100 * $stat->{ok} / ($stat->{att} || 1), $stat->{n404}, substr($tid, 0, 4));
}



sub from_db
{
  my ($dbf) = @_;
  my $db = new btindex::tdbd(file => $dbf);

  if(defined($fid)) { $db->set_it_id($fid); }
  if(defined($start)) { $db->set_it_id($start); }


  for(;;)
  {
    $con->Title($stat->{title});
    my $key = lc(ReadKey(-1) || '');
    if($key eq 'x')
    {
      print("x-key\n");
      $exit = 1;
    #}

    #if($exit)
    #{
      last;
    }


    #for(;;)
    #{
      my $tid = $db->it_id();

      if(!defined($tid)) { $exit = 2; last; }
      if(defined($fid) && $tid !~ /^$fid/i) { next; }
      if(defined($dbgot->sid($tid))) { print("0\t$tid\tgot already (db)\n"); next; }

      my $tf = btindex::torrent_path($tid);
      if(-f $tf) { print("0\t$tid\tgot already (fs)\n"); next; }

      #if(lc(ReadKey(-1) || '') eq 'x')
      #{
      #  $exit = 1;
      #  last;
      #}

      if($threads)
      {
        $q->enqueue($tid);
      }
      else
      {
        conup(p(0, $tid));
      }
    #}
  }

  if($threads)
  {
    #printf("finishing queue and threads...\n");
    $q->end();
    $_->join() foreach(@threads);
  }
}


from_db('dbs/'.$db);


printf("%d of %d (%.2f%%)\n", $stat->{ok}, $stat->{att}, 100 * $stat->{ok} / ($stat->{att} || 1), $stat->{n404});

if($count) { Whatsup->record(app => 'torrents_get', $db => $count); }
