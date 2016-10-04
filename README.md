### Case 1. Update sphinx doc
    * git checkout master
    * cd docs
    * make html
    * git checkout gh-pages
    * cp -R docs/_build/html .
    * git commit -am "update docs"
    * git push

### Case 2. Update project homepage
    * cd _source 
    * jekyll build
    * cd ..
    * cp -R _source/_site/* .
    * git commit -am "update homepage"
    * git push
