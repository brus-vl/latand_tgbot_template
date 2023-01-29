import asyncio
from contextlib import suppress

from sqlalchemy import Column, BigInteger, insert, String, ForeignKey, update, func
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from tgbot.config import load_config
from tgbot.services.database import create_db_session
from tgbot.services.db_base import Base


class User(Base):
    __tablename__ = "telegram_users"
    telegram_id = Column(BigInteger, primary_key=True)
    first_name = Column(String(length=100))
    last_name = Column(String(length=100), nullable=True)
    username = Column(String(length=100), nullable=True)
    lang_code = Column(String(length=4), default='ru_RU')
    role = Column(String(length=100), default='user')

    @classmethod
    async def get_user(cls, session_maker: sessionmaker, telegram_id: int) -> 'User':
        async with session_maker() as db_session:
            sql = select(cls).where(cls.telegram_id == telegram_id)
            request = await db_session.execute(sql)
            user: cls = request.scalar()
        return user

    @classmethod
    async def add_user(cls,
                       session_maker: sessionmaker,
                       telegram_id: int,
                       first_name: str,
                       last_name: str = None,
                       username: str = None,
                       lang_code: str = None,
                       role: str = None
                       ) -> 'User':
        async with session_maker() as db_session:
            sql = insert(cls).values(telegram_id=telegram_id,
                                     first_name=first_name,
                                     last_name=last_name,
                                     username=username,
                                     lang_code=lang_code,
                                     role=role).returning('*')
            result = await db_session.execute(sql)
            await db_session.commit()
            return result.first()

    async def update_user(self, session_maker: sessionmaker, updated_fields: dict) -> 'User':
        async with session_maker() as db_session:
            sql = update(User).where(User.telegram_id == self.telegram_id).values(**updated_fields)
            result = await db_session.execute(sql)
            await db_session.commit()
            return result

    @classmethod
    async def count_referrals(cls, session_maker: sessionmaker, user: "User"):
        async with session_maker() as db_session:
            sql = select(
                func.count(Referral.telegram_id)
            ).where(
                Referral.referrer == user.telegram_id
            ).join(
                User
            ).group_by(
                Referral.referrer
            )
            result = await db_session.execute(sql)
            return result.scalar()

    def __repr__(self):
        return f'User (ID: {self.telegram_id} - {self.first_name} {self.last_name})'


class Referral(Base):
    __tablename__ = "referral_users"
    telegram_id = Column(BigInteger, primary_key=True)
    referrer = Column(ForeignKey(User.telegram_id, ondelete='CASCADE'))

    @classmethod
    async def add_user(cls,
                       db_session: sessionmaker,
                       telegram_id: int,
                       referrer: int
                       ) -> 'User':
        async with db_session() as db_session:
            sql = insert(cls).values(
                telegram_id=telegram_id,
                referrer=referrer
            )
            result = await db_session.execute(sql)
            await db_session.commit()
            return result


if __name__ == '__main__':
    from faker import Faker
    import sqlalchemy.exc


    async def test():

        fake = Faker()
        Faker.seed(0)

        config = load_config()
        session_maker = await create_db_session(config)

        ids = [num for num in range(1, 101)]
        names = [fake.first_name() for _ in range(1, 101)]

        for user_id, first_name in zip(ids, names):
            with suppress(sqlalchemy.exc.IntegrityError):
                user = await User.add_user(session_maker, user_id, first_name)
                # user = await User.get_user(session, user_id)
                print(user)

                referrer = user

                if referrer:
                    await Referral.add_user(session_maker, user_id, referrer.telegram_id)

                refs = await User.count_referrals(session_maker, user)
                print(refs)


    asyncio.run(test())
