#perl

use LWP;
use HTTP::Request::Common;

use btindex;

use strict;

my $webapi = btindex::config('webapi');
my $ua = new LWP::UserAgent();

foreach_torrent(
  data => 'raw',
  fromzip => 1,
  sub
  {
    my ($tf, $data) = @_;

    my @tf = split('/', $tf);

    printf("%s\t", $tf[-1]);

    my $req = HTTP::Request::Common::GET($webapi.'/check/?'.$tf[-1]);
    my $res = $ua->request($req);

    if($res->code() == 200)
    {
      print("known\n");
      #return;
    }

    $req = HTTP::Request::Common::POST(
        $webapi.'/add/',
        'Content-Type' => 'application/octet-stream',
        'Content-Length' => length($data),
        Content => $data
      );

    $res = $ua->request($req);

    printf("%s\n", $res->code());
    exit;
  });

