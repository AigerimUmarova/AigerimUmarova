-- Выведите таблицу, в которой для каждого студента будет храниться его первая дата успешной (со статусом success) транзакции (из таблицы `payments`)
with first_payments as
    (select user_id
        , date_trunc('day', min(transaction_datetime::date)) as first_payment_date
    from skyeng_db.payments
    where status_name = 'success'
    group by 1
    order by 1
    ),
-- все дни 2016 года, в которые проходили уроки (из таблицы classes )
all_dates as
    (select distinct class_start_datetime::date as dt
    from skyeng_db.classes
    where class_status = 'success'
        and class_start_datetime::date between '2016-01-01' and '2016-12-31'
    group by dt
    ),
-- Сделайте агрегацию, в которой для каждого клиента на каждую дату необходимо вывести сумму поля classes (но только для успешных транзакций) (из таблицы payments)
payments_by_dates as
    (select user_id
        , transaction_datetime::date
        , sum(classes) transaction_balance_change
    from skyeng_db.payments
    where status_name = 'success'
    group by 1, 2
    order by 2
    ),
--  все даты жизни студента после того, как произошла его первая транзакция.
all_dates_by_user as
    (select f.user_id
        , l.dt
    from all_dates l
        join first_payments f on l.dt >= f.first_payment_date
    order by 1
    ),
-- Найдите изменения балансов из-за прохождения уроков.
-- Создайте CTE classes_by_dates, посчитав в таблице skyeng_db.classes количество уроков за каждый день для каждого ученика.
-- Вас не должны интересовать вводные уроки и уроки со статусом, отличным от success и failed_by_student.
-- Вы должны получить результат со следующими полями: user_id, class_date, classes (количество пройденных в этот день уроков).
-- Причем classes нужно умножить на -1, чтобы отразить, что - — это списания с баланса.
classes_by_dates as
    (select user_id
        , date_trunc('day', class_start_datetime) as class_date
        , count (id_class)*(-1) as classes
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
        and class_type != 'trial'
    group by 1, 2
    ),
-- Найдите баланс студентов, который сформирован только транзакциями.
-- Для этого объедините all_dates_by_user и payments_by_dates так, чтобы совпадали даты и user_id .
-- Используйте оконные выражения (функцию sum), чтобы найти кумулятивную сумму по полю transaction_balance_change
-- для всех строк до текущей включительно с разбивкой по user_id и сортировкой по dt.
payments_by_dates_cumsum as
    (select a.user_id
        , a.dt
        , coalesce(transaction_balance_change, 0) as transaction_balance_change
        , sum(coalesce(transaction_balance_change, 0)) over(partition by a.user_id order by a.dt rows between unbounded preceding and current row) as transaction_balance_change_cs
    from all_dates_by_user a
        left join payments_by_dates p
            on a.user_id = p.user_id
            and a.dt = p.transaction_datetime
    order by 1, 2
   ),
-- По аналогии с уже проделанным шагом для оплат создайте CTE classes_by_dates_dates_cumsum для хранения кумулятивной суммы количества пройденных уроков.
-- Для этого объедините таблицы all_dates_by_user и classes_by_dates так, чтобы совпадали даты и user_id.
-- Используйте оконные выражения (функцию sum), чтобы найти кумулятивную сумму по полю classes для всех строк до текущей включительно с разбивкой по user_id и сортировкой по dt.
-- В результате вы должны получить CTE classes_by_dates_dates_cumsumс полями: user_id, dt, classes_cs(кумулятивная сумма по classes).
-- При подсчете кумулятивной суммы обязательно нужно заменить пустые значения нулями.
classes_by_dates_dates_cumsum as
    (select a.user_id
        , a.dt
        , coalesce(classes, 0) as classes
        , sum(coalesce(classes, 0)) over(partition by a.user_id order by a.dt rows between unbounded preceding and current row) as classes_cs
    from all_dates_by_user a
        left join classes_by_dates c
            on a.user_id = c.user_id
            and a.dt = c.class_date
    order by 1, 2
    ),
-- Создайте CTE `balances` с вычисленными балансами каждого студента.
-- Для этого объедините таблицы `payments_by_dates_cumsum` и `classes_by_dates_dates_cumsum` так, чтобы совпадали даты и `user_id`.
-- Вы должны получить результат со следующими полями: `user_id`, `dt`, `transaction_balance_change`, `transaction_balance_change_cs`,
-- `classes`, `classes_cs`, `balance` (`classes_cs` + `transaction_balance_change_cs`).
-- balances as
balances as
(select b.user_id
  , b.dt
  , a.transaction_balance_change
  , a.transaction_balance_change_cs
  , b.classes
  , b.classes_cs
  , b.classes_cs + a.transaction_balance_change_cs as balance
from payments_by_dates_cumsum a
 join classes_by_dates_dates_cumsum b on a.user_id=b.user_id
    and a.dt=b.dt
)
-- select *
-- from balances
-- order by user_id, dt
-- limit 1000
select dt
  , sum(transaction_balance_change) as transaction_balance_change
  , sum(transaction_balance_change_cs) as transaction_balance_change_cs
  , sum(classes) as classes
  , sum(classes_cs) as classes_cs
  , sum(balance) as balance
 from balances
 group by dt
 order by dt
