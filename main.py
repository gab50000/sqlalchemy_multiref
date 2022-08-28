from sqlalchemy import (
    Column,
    String,
    Integer,
    create_engine,
    ForeignKey,
    inspect,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, aliased


Base = declarative_base()


class ReprMixin:
    def __repr__(self):
        inst = inspect(self)
        attrs = ", ".join(f"{c_attr.key}={c_attr.value!r}" for c_attr in inst.attrs)
        cls_name = self.__class__.__name__
        return f"{cls_name}({attrs})"


class DataCollection(Base, ReprMixin):
    __tablename__ = "datacollection"
    id = Column(Integer, primary_key=True, index=True)
    owner = Column(String, nullable=False)


class Data(Base, ReprMixin):
    __tablename__ = "data"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    collection_a_id = Column(Integer, ForeignKey("datacollection.id"))
    collection_a = relationship("DataCollection", foreign_keys=[collection_a_id])
    collection_b_id = Column(Integer, ForeignKey("datacollection.id"))
    collection_b = relationship("DataCollection", foreign_keys=[collection_b_id])
    collection_c_id = Column(Integer, ForeignKey("datacollection.id"))
    collection_c = relationship("DataCollection", foreign_keys=[collection_c_id])


engine = create_engine("sqlite:///")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def find_data_owned_by(session, user):
    DataCollA = aliased(DataCollection)
    DataCollB = aliased(DataCollection)
    DataCollC = aliased(DataCollection)

    query = (
        session.query(Data)
        .join(DataCollA, Data.collection_a)
        .join(DataCollB, Data.collection_b)
        .join(DataCollC, Data.collection_c)
        .filter(DataCollA.owner == user)
        .filter(DataCollB.owner == user)
        .filter(DataCollC.owner == user)
    )
    print(query)
    return query.all()


def main():
    data1 = Data(
        name="data1",
        collection_a=DataCollection(owner="user1"),
        collection_b=DataCollection(owner="user1"),
        collection_c=DataCollection(owner="user1"),
    )
    data2 = Data(
        name="data2",
        collection_a=DataCollection(owner="user1"),
        collection_b=DataCollection(owner="user1"),
        collection_c=DataCollection(owner="user2"),
    )

    with Session() as sess:
        sess.add(data1)
        sess.add(data2)
        sess.commit()

    with Session() as sess:
        data = find_data_owned_by(sess, "user1")
        assert len(data) == 1
        assert data[0].name == "data1"
        print(data)


if __name__ == "__main__":
    main()
