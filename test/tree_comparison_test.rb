$:.unshift(File.dirname(__FILE__) + "/../lib")
require 'test/unit'
require 'hsync'

class TreeComparisonTest < Test::Unit::TestCase
  include HSync

  # def setup
  # end

  # def teardown
  # end

  def test_comparison
   @A = {
         "a"=>
           {
             "b"=>
             {
              "file1"=> Node.new(nil, l=100, nil, nil, path="a/b/file1", "file", mtime=100),
              "file2"=> Node.new(nil, l=100, nil, nil, path="a/b/file2", "file", mtime=100),
              "file3"=> Node.new(nil, l=100, nil, nil, path="a/b/file3", "file", mtime=200),
              "file4"=> Node.new(nil, l=100, nil, nil, path="a/b/file4", "file", mtime=100)
             }
           },
         "c"=>
           {
             "file3"=> Node.new(nil, l=100, nil, nil, path="c/file3", "file", mtime=100)
           }
        }

   @B = {
         "a"=>
           {
             "b"=>
             {
              "file1"=> Node.new(nil, l=100, nil, nil, path="a/b/file1", "file", mtime=100),
              "file3"=> Node.new(nil, l=100, nil, nil, path="a/b/file3", "file", mtime=100),
              "file4"=> Node.new(nil, l=100, nil, nil, path="a/b/file4", "file", mtime=200)
             }
           },
         "d"=>
           {
             "file4"=> Node.new(nil, l=100, nil, nil, path="d/file4", "file", mtime=100)
           }
        }

    results = HSync.compare(@A, @B)
   
    ma = results.files_missing_in_a
    mb = results.files_missing_in_b   
    na = results.files_newer_in_a
    nb = results.files_newer_in_b

#     A:Source   |  B:Dest     |  Type     |  Action       
#     not exist  | exists      | File      | no action     
#     not exist  | exists      | Directory | no action     

    wanted_ma = {
         "d"=>
           {
             "file4"=> Node.new(nil, l=100, nil, nil, path="d/file4", "file", mtime=100)
           }
        }

    assert_equal(wanted_ma, ma)

#     exists     | not exist   | File      | copy A -> B   
#     exists     | not exist   | Directory | mkdir B       

    wanted_mb = {
         "a"=>
           {
             "b"=>
             {
              "file2"=> Node.new(nil, l=100, nil, nil, path="a/b/file2", "file", mtime=100)
             }
           },
         "c"=>
           {
             "file3"=> Node.new(nil, l=100, nil, nil, path="c/file3", "file", mtime=100)
           }
        }

    assert_equal(wanted_mb, mb)

#     newer      | older       | File      | copy A -> B   

    wanted_na = {
         "a"=>
           {
             "b"=>
             {
              "file3"=> Node.new(nil, l=100, nil, nil, path="a/b/file3", "file", mtime=200)
             }
           }
    }

    assert_equal(wanted_na, na)

#     older      | newer       | File      | warning       
    wanted_nb = {
         "a"=>
           {
             "b"=>
             {
              "file4"=> Node.new(nil, l=100, nil, nil, path="a/b/file4", "file", mtime=200)
             }
           }
    }

    assert_equal(wanted_nb, nb)


  end
end
