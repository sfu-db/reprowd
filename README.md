### Case 1. Update sphinx doc
    * git checkout master
    * cd docs
    * make html
    * git checkout gh-pages
    * git commit -am "update docs"
    * git push

### Case 2. Update project page
    * cd _source 
    * jekyll build
    * cd ..
    * cp -R _source/* .
    * git commit -am "update homepage"
    * git push
