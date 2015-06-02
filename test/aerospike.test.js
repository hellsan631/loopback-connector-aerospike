// This test written in mocha+should.js
var should = require('./init.js');

var Superhero, User, Post, PostWithStringId, db;

describe('Aerospike connector', function () {

    // before(function (done) {
    //
    //   User.hasMany(Post);
    //   Post.belongsTo(User);
    // });
    //
    // beforeEach(function (done) {
    //   User.destroyAll(function () {
    //     Post.destroyAll(function () {
    //       PostWithObjectId.destroyAll(function () {
    //         PostWithNumberId.destroyAll(function () {
    //           PostWithNumberUnderscoreId.destroyAll(function () {
    //             PostWithStringId.destroyAll(function () {
    //               done();
    //             });
    //           });
    //         });
    //       });
    //     });
    //   });
    // });

    describe('datasource init', function() {
        it('should respond with {}.connected true', function(done) {
            db = getDataSource();
            should(db.connected).be.ok;
            done();
        });
    })

    describe('.ping(cb)', function() {
        it('should return true for valid connection', function(done) {
            db.ping(function(success) {
                should(success).be.ok;
                done();
            });

        });

        // Test removed for now until:
        // https://github.com/aerospike/aerospike-client-nodejs/issues/58
        // is fixed, until then this test will always fail.
        //
        // it('should report connection errors', function(done) {
        //   var ds = getDataSource({
        //     host: 'localhost',
        //     port: 4 // unassigned by IANA
        //   });
        //   ds.ping(function(success) {
        //       console.log(success)
        //       should(success).equal(false);
        //       done();
        //   });
        // });
    });
    //
    // describe('Model creation', function() {
    //     it('should define a basic model', function() {
    //         Post = db.define('Post', {
    //             title: { type: String, length: 255},
    //             content: { type: String },
    //             comments: [String],
    //             image: Buffer
    //         }, {
    //             aerospike: {
    //                 idField: 'title'
    //             }
    //         });
    //     });
    //     it('define a model with basic indexes', function() {
    //         Superhero = db.define('Superhero', {
    //             name: { type: String, index: true },
    //             power: { type: String, index: true, unique: true },
    //             address: { type: String, required: false },
    //             description: { type: String, required: false },
    //             age: Number,
    //             icon: Buffer
    //         });
    //     });
    // });
    //
    // describe('Model instance creation', function() {
    //     it('create should return post with added id field', function (done) {
    //         Post.create({title: 'Post1', content: 'Post content'}, function (err, post) {
    //             should.not.exist(err);
    //             should.exist(post.id);
    //             should.exist(post.title);
    //             should.exist(post.content);
    //
    //             done();
    //         });
    //     });
    //     it('create should return post with provided id field', function (done) {
    //         Post.create({id:'foo', title: 'Post2', content: 'Post content'}, function (err, post) {
    //             should.not.exist(err);
    //             should.exist(post.id);
    //             should.exist(post.title);
    //             should.exist(post.content);
    //
    //             done();
    //         });
    //     });
    //
    //     it('should support Buffer type', function (done) {
    //         Post.create({title: 'Johns close up', content: 'here is it!', image: new Buffer('1a2')}, function (e, u) {
    //             Post.findById(u.id, function (e, post) {
    //                 post.image.should.be.an.instanceOf(Buffer);
    //                 done();
    //             });
    //         });
    //     });
    //
    // });
    //
    // describe('Model instance query', function() {
    //     it('should allow to find by id using where', function (done) {
    //         Post.create({title: 'Post1', content: 'Post1 content'}, function (err, p1) {
    //             Post.create({title: 'Post2', content: 'Post2 content'}, function (err, p2) {
    //                 Post.find({where: {id: p1.id}}, function (err, p) {
    //                     should.not.exist(err);
    //                     should.exist(p && p[0]);
    //                     p.length.should.be.equal(1);
    //                     // Not strict equal
    //                     p[0].id.should.be.eql(p1.id);
    //                     done();
    //                 });
    //             });
    //         });
    //     });
    //
    //     it('should allow to find by bin using where', function (done) {
    //         Post.create({title: 'Post1', content: 'Post1 content'}, function (err, p1) {
    //             Post.create({title: 'Post2', content: 'Post2 content'}, function (err, p2) {
    //                 Post.create({title: 'Post3', content: 'Post2 content'}, function (err, p2) {
    //                     Post.find({where: {content: 'Post2 content'}}, function (err, p) {
    //                         should.not.exist(err);
    //                         should.exist(p && p[0]);
    //                         p.length.should.be.equal(2);
    //                         // Not strict equal
    //                         p[0].id.should.be.eql(p1.id);
    //                         done();
    //                     });
    //                 });
    //             });
    //         });
    //     });
    //
    //     it('should allow to find by bin using where when bin is mapped to the id', function (done) {
    //         Post.create({title: 'Post1', content: 'Post1 content'}, function (err, p1) {
    //             Post.find({where: {title: 'Post 1'}}, function (err, p) {
    //                 should.not.exist(err);
    //                 should.exist(p && p[0]);
    //                 p.length.should.be.equal(1);
    //                 // Not strict equal
    //                 p[0].id.should.be.eql(p1.id);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should allow to find by id using where inq', function (done) {
    //         Post.create({title: 'Post1', content: 'Post1 content'}, function (err, p1) {
    //             Post.create({title: 'Post2', content: 'Post2 content'}, function (err, p2) {
    //                 Post.find({where: {id: {inq: [p1.id]}}}, function (err, p) {
    //                     should.not.exist(err);
    //                     should.exist(p && p[0]);
    //                     p.length.should.be.equal(1);
    //                     // Not strict equal
    //                     p[0].id.should.be.eql(p1.id);
    //                     done();
    //                 });
    //             });
    //         });
    //     });
    //
    //
    //
    //     it('should support "and" operator that is satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {and: [{title: 'My Post'}, {content: 'Hello'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 1);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support "and" operator that is not satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {and: [{title: 'My Post'}, {content: 'Hello1'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 0);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support "or" that is satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {or: [{title: 'My Post'}, {content: 'Hello1'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 1);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support "or" operator that is not satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {or: [{title: 'My Post1'}, {content: 'Hello1'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 0);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support "nor" operator that is satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {nor: [{title: 'My Post1'}, {content: 'Hello1'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 1);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support "nor" operator that is not satisfied', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {nor: [{title: 'My Post'}, {content: 'Hello1'}]}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 0);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support neq for match', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {title: {neq: 'XY'}}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 1);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     it('should support neq for no match', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.find({where: {title: {neq: 'My Post'}}}, function (err, posts) {
    //                 should.not.exist(err);
    //                 posts.should.have.property('length', 0);
    //                 done();
    //             });
    //         });
    //     });
    //
    //     // The where object should be parsed by the connector
    //     it('should support where for count', function (done) {
    //         Post.create({title: 'My Post', content: 'Hello'}, function (err, post) {
    //             Post.count({and: [{title: 'My Post'}, {content: 'Hello'}]}, function (err, count) {
    //                 should.not.exist(err);
    //                 count.should.be.equal(1);
    //                 Post.count({and: [{title: 'My Post1'}, {content: 'Hello'}]}, function (err, count) {
    //                     should.not.exist(err);
    //                     count.should.be.equal(0);
    //                     done();
    //                 });
    //             });
    //         });
    //     });
    //
    //     // The where object should be parsed by the connector
    //     it('should support where for destroyAll', function (done) {
    //         Post.create({title: 'My Post1', content: 'Hello'}, function (err, post) {
    //             Post.create({title: 'My Post2', content: 'Hello'}, function (err, post) {
    //                 Post.destroyAll({and: [
    //                     {title: 'My Post1'},
    //                     {content: 'Hello'}
    //                 ]}, function (err) {
    //                     should.not.exist(err);
    //                     Post.count(function (err, count) {
    //                         should.not.exist(err);
    //                         count.should.be.equal(1);
    //                         done();
    //                     });
    //                 });
    //             });
    //         });
    //     });
    // });


    // after(function (done) {
    //     User.destroyAll(function () {
    //         Post.destroyAll(function () {
    //             PostWithObjectId.destroyAll(function () {
    //                 PostWithNumberId.destroyAll(function () {
    //                     PostWithNumberUnderscoreId.destroyAll(done);
    //                 });
    //             });
    //         });
    //     });
    // });
});
