"""
The service responsible for grouping collections of variables into querysets

It reflects information from the "main db", making groups of columns that can be
used for further querying.
"""
import os
from typing import Union
import enum
import contextlib

import fastapi
from fastapi import Response,Request

from sqlalchemy import create_engine,Column,String,Integer,Enum,ForeignKey,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,relationship

import pydantic

from environs import Env

env = Env()
env.read_env()

app = fastapi.FastAPI()
engine = create_engine("sqlite:///db.sqlite")

Base = declarative_base()

Session = sessionmaker(bind=engine)

class UnitOfAnalysis(enum.Enum):
    """
    Enum representing the current valid units of analysis
    """
    priogrid_cell = "pg"
    country = "ctry"

class MainTable(Base):
    """
    Entry representing a Table in the main DB.
    """
    __tablename__="maintable"
    name = Column(String,primary_key=True)
    uoa = Column(Enum(UnitOfAnalysis),nullable=False)
    variables = relationship("MainVariable",back_populates="table")

class MainVariable(Base):
    """
    Entries corresponding to columns in the relevant tables in the DB.
    Constraint checking against columns is hard, so i've instead
    opted to make this system self-healing, meaning it fails gracefully
    if a column does not actually exist in the information_schema.
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
        return True

class QuerySet(Base):
    """
    A queryset, which is a collection of variables
    """
    __tablename__ = "queryset"

    #queryset_id = Column(Integer,primary_key=True,autoincrement=True)
    name = Column(String,primary_key=True)

    variables = relationship("MainVariable",secondary="querysets_variables")
    def remote_validate(self):
        """
        Check if registered variables are available remotely
        """
        for variable in self.variables:
            if variable.remote_validate():
                pass
            else:
                variable.delete()

querysets_variables = Table("querysets_variables",Base.metadata,
        Column("variable_id",Integer,ForeignKey("variable.variable_id")),
        Column("queryset_name",String,ForeignKey("queryset.name"))
        )

Base.metadata.create_all(engine)

def hyperlink(request:Request,path:str,key:Union[str,int])->str:
    """
    Composes a hyperlink from the provided components
    """
    return os.path.join(request.url.hostname,path,key)

@app.get("/queryset/{name}")
def qset_detail(request:Request,name:str):
    """
    Return queryset contents
    """
    with contextlib.closing(Session()) as sess:
        queryset = sess.query(QuerySet).get(name)
        if queryset is None:
            return Response(status_code=404)
        else:
            queryset.remote_validate()
            variables = [hyperlink(request,"variable",v.name) for v in queryset.variables]
    return {
        "name": queryset.name,
        "variables":variables
    }
    #return [VariableResponse.from_object(v) for v in variables] 

@app.get("/queryset/")
def qset_list(request:Request):
    """
    Returns a list of querysets
    """
    with contextlib.closing(Session()) as sess:
        querysets = sess.query(QuerySet).all()
        serialized = []
        for queryset in querysets:
            serialized.append({
                    "name": queryset.name,
                    "url": hyperlink(request,"queryset",queryset.name),
                    "variables": len(queryset.variables),
                })
    return serialized

"""
@app.get("/variable/{pk}")
def variable_detail(pk:int):
    pass

@app.get("/variable/")
def variable_list():
    pass

@app.get("/table/{name}")
def table_detail(name:str):
    pass

@app.get("/table/")
def table_list():
    pass
"""
