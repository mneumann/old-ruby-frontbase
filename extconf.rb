if VERSION < "1.3"
  print "This library is for ruby-1.3 or higher.\n"
  exit 1
end

require "mkmf"

dir_config('frontbase')

$LDFLAGS = "-framework FBCAccess"

create_makefile("frontbase")

