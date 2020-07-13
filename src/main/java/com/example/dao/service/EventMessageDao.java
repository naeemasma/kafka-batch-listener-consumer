package com.example.dao.service;

import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.example.domain.EventMessage;

@Mapper
public interface EventMessageDao {

	@Select("select * from eventMessages")
	public List<EventMessage> findAll();

	@Select("SELECT * FROM eventMessages WHERE id = #{id}")
	public EventMessage findById(int id);

	@Delete("DELETE FROM eventMessages WHERE id = #{id}")
	public int deleteById(long id);

	@Insert("INSERT INTO eventMessages(description, severity) "
			+ " VALUES (#{description}, #{severity})")
	@Options(useGeneratedKeys = true, keyColumn = "id", keyProperty = "id")
	public int insert(EventMessage eventMessage);

	@Update("Update eventMessages set description=#{description}, severity=#{severity}) "
			+ " where id=#{id}")
	public int update(EventMessage eventMessage);
	
}
