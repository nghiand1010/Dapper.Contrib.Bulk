using Dapper.Contrib.Bulk.Extensions;
using Dapper.Contrib.Extensions;
using NpgsqlTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TableAttribute = Dapper.Contrib.Extensions.TableAttribute;

namespace Dapper.Tests.Contrib.Bulk
{
    [Table("ObjectX")]
    public class ObjectX
    {
        [ExplicitKey]
        public string ObjectXId { get; set; }
        public string Name { get; set; }
    }

    [Table("ObjectY")]
    public class ObjectY
    {
        [ExplicitKey]
        public int ObjectYId { get; set; }
        public string Name { get; set; }
    }

    [Table("ObjectZ")]
    public class ObjectZ
    {
        [ExplicitKey]
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public interface IUser
    {
        [Key]
        int id { get; set; }
        string name { get; set; }
        int age { get; set; }
    }

    public class User : IUser
    {

        [Key]
        public int id { get; set; }
        public string name { get; set; }
        public int age { get; set; }
    }

    [Table("Users")]
    public class UserChangeColumn 
    {

        [Key]
        public int id { get; set; }

        [ColumnName("name")]
        public string fullname { get; set; }
        public int age { get; set; }
    }


    public interface INullableDate
    {
        [Key]
        int Id { get; set; }
        DateTime? DateValue { get; set; }
    }

    public class NullableDate : INullableDate
    {
        public int Id { get; set; }
        public DateTime? DateValue { get; set; }
    }

    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    [Table("Stuff")]
    public class Stuff
    {
        [Key]
        public short TheId { get; set; }
        public string Name { get; set; }
        public DateTime? Created { get; set; }
    }

    [Table("Automobiles")]
    public class Car
    {
        public int Id { get; set; }
        public string Name { get; set; }
        [Computed]
        public string Computed { get; set; }
    }

    [Table("Results")]
    public class Result
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Order { get; set; }
    }

    [Table("GenericType")]
    public class GenericType<T>
    {
        [ExplicitKey]
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public abstract partial class TestSuite
    {
        public abstract IDbConnection GetConnection();

        protected static string GetConnectionString(string name, string defaultConnectionString) =>
            Environment.GetEnvironmentVariable(name) ?? defaultConnectionString;

        private IDbConnection GetOpenConnection()
        {
            var connection = GetConnection();
            connection.Open();
            return connection;
        }

        [Fact]
        public void TestChangeColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                List<UserChangeColumn> users = new List<UserChangeColumn>
                {
                    new UserChangeColumn { fullname = "Adama", age = 10 },
                    new UserChangeColumn { fullname = "Adama2", age = 10 }
                };
                connection.BulkInsert(users);

                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }


        [Fact]
        public void TestInsertNormal()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.Insert(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }

        [Fact]
        public void TestInsertNormalEmpty()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                connection.Insert(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }

        [Fact]
        public void TestInsertBulkEmpty()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                connection.BulkInsert(users);

                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }

        [Fact]
        public void TestInsertBulk()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);

                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }

        [Fact]
        public async Task TestInsertBulkAsyncEmpty()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                await connection.BulkInsertAsync(users);

                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }

        [Fact]
        public async Task TestInsertBulkAsync()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                await connection.BulkInsertAsync(users);

                var insertedUsers = connection.GetAll<User>();
                Assert.Equal(users.Count, insertedUsers.Count());
            }
        }


        [Fact]
        public void TestUpDateNormal()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);
                var insertedUsers = connection.GetAll<User>();
                foreach (var item in insertedUsers)
                {
                    item.name = item.name + "_update_" + item.age;
                }
                connection.Update(insertedUsers);

                var updateUsers = connection.GetAll<User>().ToDictionary(x => x.id, x => x);

                bool isTrue = true;
                foreach (var item in insertedUsers)
                {
                    if (updateUsers.ContainsKey(item.id))
                    {
                        var user = updateUsers[item.id];
                        isTrue=isTrue && (item.id == user.id && item.name == user.name && item.age == user.age);
                    }
                    else
                    {
                        Assert.True(false);
                    }
                }

                Assert.True(isTrue);
            }
        }

        [Fact]
        public void TestUpdateBulk()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);
                var insertedUsers = connection.GetAll<User>();
                foreach (var item in insertedUsers)
                {
                    item.name = item.name + "_update_" + item.age;
                }
                connection.BulkUpdate(insertedUsers);
                var updateUsers = connection.GetAll<User>().ToDictionary(x => x.id, x => x);

                bool isTrue = true;
                foreach (var item in insertedUsers)
                {
                    if (updateUsers.ContainsKey(item.id))
                    {
                        var user = updateUsers[item.id];
                        isTrue = isTrue && (item.id == user.id && item.name == user.name && item.age == user.age);
                    }
                    else
                    {
                        Assert.True(false);
                    }
                }

                Assert.True(isTrue);
            }
        }


        [Fact]
        public void TestUpdateBulkEmpty()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                connection.BulkInsert(users);
                var insertedUsers = connection.GetAll<User>();
                foreach (var item in insertedUsers)
                {
                    item.name = item.name + "_update_" + item.age;
                }
                connection.BulkUpdate(insertedUsers);
                var updateUsers = connection.GetAll<User>().ToDictionary(x => x.id, x => x);

                bool isTrue = true;
                foreach (var item in insertedUsers)
                {
                    if (updateUsers.ContainsKey(item.id))
                    {
                        var user = updateUsers[item.id];
                        isTrue = isTrue && (item.id == user.id && item.name == user.name && item.age == user.age);
                    }
                    else
                    {
                        Assert.True(false);
                    }
                }

                Assert.True(isTrue);
            }
        }


        [Fact]
        public async Task TestUpdateBulkAsyncEmpty()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                await connection.BulkInsertAsync(users);
                var insertedUsers = connection.GetAll<User>();
                foreach (var item in insertedUsers)
                {
                    item.name = item.name + "_update_" + item.age;
                }
                await connection.BulkUpdateAsync(insertedUsers);
                var updateUsers = connection.GetAll<User>().ToDictionary(x => x.id, x => x);

                bool isTrue = true;
                foreach (var item in insertedUsers)
                {
                    if (updateUsers.ContainsKey(item.id))
                    {
                        var user = updateUsers[item.id];
                        isTrue = isTrue && (item.id == user.id && item.name == user.name && item.age == user.age);
                    }
                    else
                    {
                        Assert.True(false);
                    }
                }

                Assert.True(isTrue);
            }
        }

        [Fact]
        public async Task TestUpdateBulkAsync()
        {
            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                await connection.BulkInsertAsync(users);
                var insertedUsers = connection.GetAll<User>();
                foreach (var item in insertedUsers)
                {
                    item.name = item.name + "_update_" + item.age;
                }
                await connection.BulkUpdateAsync(insertedUsers);
                var updateUsers = connection.GetAll<User>().ToDictionary(x => x.id, x => x);

                bool isTrue = true;
                foreach (var item in insertedUsers)
                {
                    if (updateUsers.ContainsKey(item.id))
                    {
                        var user = updateUsers[item.id];
                        isTrue = isTrue && (item.id == user.id && item.name == user.name && item.age == user.age);
                    }
                    else
                    {
                        Assert.True(false);
                    }
                }

                Assert.True(isTrue);
            }
        }


        [Fact]
        public void TestDeleteBulk()
        {

            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);
                users = connection.GetAll<User>().ToList();
                connection.BulkDelete(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Empty(insertedUsers);
            }
        }

        [Fact]
        public async Task TestDeleteBulkAsync()
        {

            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);
                users = connection.GetAll<User>().ToList();
                await connection.BulkDeleteAsync(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Empty(insertedUsers);
            }
        }

        [Fact]
        public void TestDeleteBulkEmpty()
        {

            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                connection.BulkInsert(users);
                users = connection.GetAll<User>().ToList();
                connection.BulkDelete(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Empty(insertedUsers);
            }
        }

        [Fact]
        public async Task TestDeleteBulkAsyncEmpty()
        {

            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetEmptyUser();
                connection.BulkInsert(users);
                users = connection.GetAll<User>().ToList();
                await connection.BulkDeleteAsync(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Empty(insertedUsers);
            }
        }

        [Fact]
        public void TestDeleteNormal()
        {

            using (var connection = GetOpenConnection())
            {
                connection.DeleteAll<User>();
                var users = GetTempUser();
                connection.BulkInsert(users);
                users=connection.GetAll<User>().ToList();
                connection.Delete(users);
                var insertedUsers = connection.GetAll<User>();
                Assert.Empty(insertedUsers);
            }
        }


      

        private List<User> GetTempUser()
        {
            List<User> users = new List<User>
                {
                    new User { name = "Adama", age = 10 },
                    new User { name = "Adama2", age = 10 }
                };

            for (int i = 0;i<500;i++)
            {
                users.Add(
                    new User { name = "Adama" + i, age = i % 20 });
            }

            return users;
        }

        private List<User> GetEmptyUser()
        {
            List<User> users = new List<User>
                {
                    
                };
            return users;
        }

    }
}
