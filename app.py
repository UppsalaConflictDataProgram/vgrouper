"""
The service responsible for grouping collections of variables into querysets

It reflects information from the "main db", making groups of columns that can be
used for further querying.
"""
import os
from typing import Union,List,Optional,Tuple
import enum
import contextlib
from contextlib import closing

import fastapi
from fastapi import Response,Request
from starlette.responses import JSONResponse

import requests

from sqlalchemy import create_engine,Column,String,Integer,Enum,ForeignKey,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,relationship

import pydantic

from environs import Env

env = Env()
env.read_env()

engine = create_engine("sqlite:///db.sqlite")
Base = declarative_base()
Session = sessionmaker(bind=engine)

# ========================================================

def exists_remotely(table_name,column_name=None):
    """
    Check if table / table column exists in the DB
    """
    path = [env("BROKER_URL"),"table",table_name]
    if column_name is not None:
        path.append(column_name)

    result = requests.get(os.path.join(*path))
    if result.status_code == 404:
        return False
    elif result.status_code == 200:
        return True
    else:
        raise ValueError(f"Unexpected status code from broker: {result.status_code}")

def hyperlink(request:Request,path:str,key:Union[str,int])->str:
    """
    Composes a hyperlink from the provided components
    """
    base = f"{request.url.scheme}://{request.url.hostname}:{request.url.port}"
    return os.path.join(base,path,key)

# ========================================================

class MainTable(Base):
    """
    Entry representing a Table in the main DB.
    """
    __tablename__="maintable"
    name = Column(String,primary_key=True)
    #uoa = Column(Enum(UnitOfAnalysis),nullable=False)
    variables = relationship("MainColumn",back_populates="table")
    querysets = relationship("QuerySet",back_populates="table")

class MainColumn(Base):
    """
    Entries corresponding to columns in the relevant tables in the DB.
    Constraint checking against columns is hard, so i've instead
    opted to make this system self-healing, meaning it does a consistency
    check whenever a queryset is retrieved.
    """
    __tablename__ = "variable"

    variable_id = Column(Integer,primary_key=True,autoincrement=True)
    name = Column(String,nullable=False)

    table_name = Column(String,ForeignKey("maintable.name"))
    table = relationship("MainTable",back_populates="variables")

    def remote_validate(self):
        """
        Check if a variable is available remotely
        """
        url = os.path.join(env("BROKER_URL"),"table",self.table_name,self.name)
        response = requests.get(url)
        return response.status_code == 200

class QuerySet(Base):
    """
    A queryset, which is a collection of variables
    """
    __tablename__ = "queryset"

    #queryset_id = Column(Integer,primary_key=True,autoincrement=True)
    name = Column(String,primary_key=True)

    table_name = Column(String,ForeignKey("maintable.name"))
    table = relationship("MainTable",back_populates="querysets")

    variables = relationship("MainColumn",secondary="querysets_variables")

    def remote_validate(self):
        """
        Check if registered variables are available remotely
        """
        valid = exists_remotely(self.table_name)
        for variable in self.variables:
            valid &= exists_remotely(self.table_name,variable.name)
        return valid

    def remote_repair(self):
        session = Session.object_session(self)
        if not exists_remotely(self.table_name):
            session.delete(self)
            session.commit()
        else:
            for variable in self.variables:
                if variable.remote_validate():
                    pass
                else:
                    sess = Session.object_session(self)
                    sess.delete(variable)
                sess.commit()

querysets_variables = Table("querysets_variables",Base.metadata,
        Column("variable_id",Integer,ForeignKey("variable.variable_id")),
        Column("queryset_name",String,ForeignKey("queryset.name"))
        )

Base.metadata.create_all(engine)

# ========================================================

class ColumnPost(pydantic.BaseModel):
    """
    A posted column 
    """
    name: str

class QuerySetPost(pydantic.BaseModel):
    """
    A posted QuerySet
    """
    name: str
    columns: List[ColumnPost]
    table: str

class QuerySetUpdate(pydantic.BaseModel):
    """
    A posted QuerySet
    """
    columns: List[ColumnPost]

def validate_qs_post(qs)->Tuple[bool,Optional[JSONResponse]]:
    try:
        assert exists_remotely(qs.table)
    except AssertionError:
        return False, JSONResponse({"message":f"Table {qs.table} does not exist"},status_code=400)

    if qs.columns is not None:
        for c in qs.columns:
            try:
                assert exists_remotely(qs.table,c.name)
            except AssertionError:
                return False, JSONResponse({
                        "message":f"Column {c.name} does not exist"
                    },status_code=400)

    return True,None

# ========================================================

app = fastapi.FastAPI()

@app.delete("/queryset/{name}")
def qset_delete(name:str):
    with closing(Session()) as sess:
        existing = sess.query(QuerySet).get(name)
        if existing is None:
            return Response(status_code=404)
        else:
            sess.delete(existing)
            sess.commit()
            return Response(status_code=204)

@app.put("/queryset/{name}")
def qset_update(request:Request,queryset:QuerySetUpdate,name:str):
    with closing(Session()) as sess:
        existing = sess.query(QuerySet).get(name)
        if not existing:
            return Response(status_code=404)

        columns = []
        for column in queryset.columns:
            try:
                assert exists_remotely(existing.table_name,column.name)
            except AssertionError:
                return JSONResponse({"message":f"Column {column.name} does not exist"},status_code=400)

            column_object = (sess.query(MainColumn)
                    .filter(
                        MainColumn.table_name==existing.table_name,
                        MainColumn.name==column.name)
                    .first()
                )
            if column_object is None:
                column_object = MainColumn(name=column.name,table = existing.table)
            columns.append(column_object) 

        existing.variables = columns
        sess.commit()
        return Response(status_code=204)


@app.post("/queryset/")
def qset_create(request:Request,queryset:QuerySetPost):
    with closing(Session()) as sess:
        exists = sess.query(QuerySet).get(queryset.name) is not None
        if exists:
            return Response(status_code=409)

        try:
            assert exists_remotely(queryset.table_name)
        except AssertionError:
            return JSONResponse({"message":f"Table {queryset.table_name} does not exist"},
                    status_code=400)
        for c in queryset.columns:
            try:
                assert exists_remotely(queryset.table_name,c.name)
            except AssertionError:
                return JSONResponse({"message":f"Column {c.name} does not exist"},
                        status_code=400)

        table = sess.query(MainTable).get(queryset.table)
        if table is None:
            table = MainTable(name=queryset.name)
            sess.add(table)

        columns = []
        for col in queryset.columns:
            column = (sess.query(MainColumn)
                    .filter(MainColumn.name==col.name,MainColumn.table==table)
                    .first()
                )
            if column is None:
                column = MainColumn(name=col.name,table=table)
                sess.add(column)

            columns.append(column)

        queryset = QuerySet(
            name = queryset.name,
            table_name = queryset.table,
            variables = columns
        )

        sess.add(queryset)
        sess.commit()
        return {"queryset":hyperlink(request,"queryset",queryset.name)}


@app.get("/queryset/{name}")
def qset_detail(request:Request,name:str):
    """
    Return queryset contents
    """
    with closing(Session()) as sess:
        queryset = sess.query(QuerySet).get(name)
        if queryset is None:
            return Response(status_code=404)
        return {
            "queryset_name": queryset.name,
            "table_name": queryset.table_name,
            "variables":[v.name for v in queryset.variables]
        }

@app.get("/queryset/")
def qset_list(request:Request):
    """
    Returns a list of querysets
    """
    with closing(Session()) as sess:
        querysets = sess.query(QuerySet).all()
        serialized = []
        for queryset in querysets:
            serialized.append({
                    "name": queryset.name,
                    "url": hyperlink(request,"queryset",queryset.name),
                    "variables": len(queryset.variables),
                })
    return serialized
